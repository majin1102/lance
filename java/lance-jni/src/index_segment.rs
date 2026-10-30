// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi};
use arrow_array::{Array, RecordBatch, StructArray, UInt64Array};
use arrow_schema::{DataType, Schema};
use datafusion_common::ScalarValue;
use futures::{StreamExt, stream};
use jni::JNIEnv;
use jni::objects::{JByteArray, JObject, JString};
use jni::sys::{jbyteArray, jint, jlong, jlongArray};
use lance::index::DatasetIndexExt;
use lance::index::segment::{IndexSegmentReadTask, SegmentRowIdMode};
use lance_core::ROW_ADDR_FIELD;
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::scalar::SargableQuery;
use lance_io::stream::RecordBatchStreamAdapter;
use lance_table::format::{IndexMetadata, pb};
use object_store::path::Path;
use prost::Message;
use uuid::Uuid;

use crate::block_on;
use crate::blocking_dataset::{BlockingDataset, NATIVE_DATASET};
use crate::error::{Error, Result};
use crate::primitive_scan::{export_stream, resolve_runtime, validate_stream_address};

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_lance_index_IndexSegmentReadTask_captureNative(
    mut env: JNIEnv,
    _class: JObject,
    dataset: JObject,
    uuid: JString,
) -> jbyteArray {
    let result: Result<jbyteArray> = (|| {
        let uuid: String = env.get_string(&uuid)?.into();
        let uuid = Uuid::parse_str(&uuid)
            .map_err(|error| Error::input_error(format!("Invalid index UUID {uuid:?}: {error}")))?;
        let dataset = {
            let guard =
                unsafe { env.get_rust_field::<_, _, BlockingDataset>(dataset, NATIVE_DATASET) }?;
            guard.inner.clone()
        };
        let task = block_on(async {
            let indices = dataset.load_indices().await?;
            if dataset.manifest().uses_stable_row_ids()
                || indices.iter().any(lance_index::is_system_index)
                || dataset
                    .fragments()
                    .iter()
                    .any(|fragment| !fragment.overlays.is_empty())
            {
                return Err(Error::unsupported_error(
                    "Segment capture requires physical row addresses without system indices, \
                     fragment reuse or overlays"
                        .to_string(),
                ));
            }
            let metadata = indices
                .iter()
                .find(|metadata| metadata.uuid == uuid)
                .ok_or_else(|| {
                    Error::input_error(format!("Index segment {uuid} is absent from the snapshot"))
                })?;
            if metadata.base_id.is_some() {
                return Err(Error::unsupported_error(
                    "Segment capture does not support external index bases; resolve the base and \
                     use fromSegment instead"
                        .to_string(),
                ));
            }
            Ok::<_, Error>(IndexSegmentReadTask::from_segment(
                dataset.uri(),
                dataset.indices_dir().join(uuid.to_string()),
                Arc::new(metadata.clone()),
                SegmentRowIdMode::PhysicalAddresses,
            )?)
        })?;
        Ok(env.byte_array_from_slice(&task.encode())?.into_raw())
    })();
    ok_or_throw_with_return!(env, result, std::ptr::null_mut())
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_lance_index_IndexSegmentReadTask_fromSegmentNative(
    mut env: JNIEnv,
    _class: JObject,
    uri: JString,
    path: JString,
    metadata: JByteArray,
    row_id_mode: JString,
) -> jbyteArray {
    let result: Result<jbyteArray> = (|| {
        let uri: String = env.get_string(&uri)?.into();
        let path: String = env.get_string(&path)?.into();
        let row_id_mode: String = env.get_string(&row_id_mode)?.into();
        let row_id_mode = match row_id_mode.as_str() {
            "physical_addresses" => SegmentRowIdMode::PhysicalAddresses,
            "stable_ids" => SegmentRowIdMode::StableIds,
            "requires_remapping" => SegmentRowIdMode::RequiresRemapping,
            value => return Err(Error::input_error(format!("Unknown row-ID mode {value:?}"))),
        };
        let metadata = pb::IndexMetadata::decode(env.convert_byte_array(&metadata)?.as_slice())
            .map_err(lance_core::Error::from)?;
        let path = Path::parse(&path).map_err(|error| {
            Error::input_error(format!("Invalid segment path {path:?}: {error}"))
        })?;
        let task = IndexSegmentReadTask::from_segment(
            &uri,
            path,
            Arc::new(IndexMetadata::try_from(metadata)?),
            row_id_mode,
        )?;
        Ok(env.byte_array_from_slice(&task.encode())?.into_raw())
    })();
    ok_or_throw_with_return!(env, result, std::ptr::null_mut())
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_lance_index_IndexSegmentReadTask_validateNative(
    mut env: JNIEnv,
    _class: JObject,
    payload: JByteArray,
) {
    let result: Result<()> = (|| {
        IndexSegmentReadTask::decode(&env.convert_byte_array(&payload)?)?;
        Ok(())
    })();
    ok_or_throw_without_return!(env, result);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_lance_index_IndexSegmentReadTask_fragmentIdsNative(
    mut env: JNIEnv,
    _class: JObject,
    payload: JByteArray,
) -> jlongArray {
    let result: Result<jlongArray> = (|| {
        let task = IndexSegmentReadTask::decode(&env.convert_byte_array(&payload)?)?;
        let coverage = task.metadata().fragment_bitmap.as_ref().ok_or_else(|| {
            Error::input_error("Index segment is missing fragment coverage".to_string())
        })?;
        let length = i32::try_from(coverage.len()).map_err(|_| {
            Error::input_error("Fragment coverage exceeds Java array capacity".to_string())
        })?;
        let fragments = coverage.iter().map(i64::from).collect::<Vec<_>>();
        let array = env.new_long_array(length)?;
        env.set_long_array_region(&array, 0, &fragments)?;
        Ok(array.into_raw())
    })();
    ok_or_throw_with_return!(env, result, std::ptr::null_mut())
}

fn import_literal(array_address: jlong, schema_address: jlong) -> Result<ScalarValue> {
    if array_address == 0 || schema_address == 0 {
        return Err(Error::input_error(
            "Equality requires non-null ArrowArray and ArrowSchema addresses".to_string(),
        ));
    }
    // Arrow Java does not guarantee Rust struct alignment. Move both exported
    // owners with unaligned operations and leave empty structs for Java close().
    let (array, schema) = unsafe {
        let array = std::ptr::read_unaligned(array_address as *const FFI_ArrowArray);
        std::ptr::write_unaligned(
            array_address as *mut FFI_ArrowArray,
            FFI_ArrowArray::empty(),
        );
        let schema = std::ptr::read_unaligned(schema_address as *const FFI_ArrowSchema);
        std::ptr::write_unaligned(
            schema_address as *mut FFI_ArrowSchema,
            FFI_ArrowSchema::empty(),
        );
        (array, schema)
    };
    let data = unsafe { from_ffi(array, &schema) }?;
    if !matches!(data.data_type(), DataType::Struct(_)) {
        return Err(Error::input_error(
            "Equality literal must be an Arrow record batch".to_string(),
        ));
    }
    let literal = StructArray::from(data);
    if literal.len() != 1 || literal.num_columns() != 1 {
        return Err(Error::input_error(format!(
            "Equality literal needs one column and one row; got {} columns and {} rows",
            literal.num_columns(),
            literal.len(),
        )));
    }
    ScalarValue::try_from_array(literal.column(0).as_ref(), 0)
        .map_err(|error| Error::input_error(format!("Invalid equality literal: {error}")))
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_lance_index_IndexSegmentReadTask_exportNative(
    mut env: JNIEnv,
    _class: JObject,
    payload: JByteArray,
    session: JObject,
    storage_options: JObject,
    namespace: JObject,
    table_id: JObject,
    block_size: jint,
    batch_size: jint,
    query_type: JString,
    literal_array: jlong,
    literal_schema: jlong,
    stream_address: jlong,
) {
    let result: Result<()> = (|| {
        validate_stream_address(stream_address)?;
        let batch_size = usize::try_from(batch_size)
            .ok()
            .filter(|size| *size > 0)
            .ok_or_else(|| {
                Error::input_error(format!("batchSize must be positive, got {batch_size}"))
            })?;
        let task = IndexSegmentReadTask::decode(&env.convert_byte_array(&payload)?)?;
        let query_type: String = env.get_string(&query_type)?.into();
        let query = match query_type.as_str() {
            "equals" => {
                let literal = import_literal(literal_array, literal_schema)?;
                // SQL '=' with a NULL literal is UNKNOWN for every row.
                (!literal.is_null()).then_some(SargableQuery::Equals(literal))
            }
            "is_null" => Some(SargableQuery::IsNull()),
            value => {
                return Err(Error::input_error(format!(
                    "Unknown segment query {value:?}"
                )));
            }
        };
        let (session, params) = resolve_runtime(
            &mut env,
            &session,
            &storage_options,
            &namespace,
            &table_id,
            block_size,
        )?;
        let (scanner, matches) = block_on(async {
            let scanner = task
                .open_with_session(Some(params), session.clone())
                .await?;
            let matches = match query {
                Some(query) => Some(scanner.search(&query, &NoOpMetricsCollector).await?),
                None => None,
            };
            if matches
                .as_ref()
                .is_some_and(|rows| rows.row_addrs().is_none())
            {
                return Err(Error::unsupported_error(
                    "Segment query must produce explicit physical row addresses".to_string(),
                ));
            }
            Ok::<_, Error>((scanner, matches))
        })?;
        let schema = Arc::new(Schema::new(vec![ROW_ADDR_FIELD.clone()]));
        let stream_schema = schema.clone();
        let addresses = matches.into_iter().flat_map(|rows| rows.into_addr_iter());
        let batches = stream::unfold(
            (addresses, scanner, session),
            move |(mut addresses, scanner, session)| {
                let schema = stream_schema.clone();
                async move {
                    let values = addresses.by_ref().take(batch_size).collect::<Vec<_>>();
                    if values.is_empty() {
                        return None;
                    }
                    let batch =
                        RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(values))])
                            .map_err(lance_core::Error::from);
                    Some((batch, (addresses, scanner, session)))
                }
            },
        )
        .boxed();
        export_stream(
            RecordBatchStreamAdapter::new(schema, batches),
            stream_address,
        )
    })();
    ok_or_throw_without_return!(env, result);
}
