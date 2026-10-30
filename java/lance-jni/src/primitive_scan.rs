// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use jni::JNIEnv;
use jni::objects::{JByteArray, JLongArray, JMap, JObject, JString};
use jni::sys::{jbyteArray, jint, jlong};
use lance::dataset::fragment::FragmentReader;
use lance::dataset::fragment::read_task::FragmentReadTask;
use lance::session::Session;
use lance_core::datatypes::Schema;
use lance_file::datatypes::{Fields, FieldsWithMeta};
use lance_io::ffi::to_ffi_arrow_array_stream;
use lance_io::object_store::{
    LanceNamespaceStorageOptionsProvider, ObjectStoreParams, StorageOptionsAccessor,
    StorageOptionsProvider,
};
use lance_io::stream::RecordBatchStream;
use lance_table::format::Fragment;
use prost::Message;

use crate::blocking_dataset::{BlockingDataset, NATIVE_DATASET, extract_namespace_info};
use crate::error::{Error, Result};
use crate::session::session_from_handle;
use crate::utils::to_rust_map;
use crate::{JNIEnvExt, RT, block_on};

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_lance_fragment_FragmentReadTask_captureNative(
    mut env: JNIEnv,
    _class: JObject,
    dataset: JObject,
    fragment_id: jlong,
    projection: JObject,
) -> jbyteArray {
    ok_or_throw_with_return!(
        env,
        capture(&mut env, dataset, fragment_id, projection),
        std::ptr::null_mut()
    )
}

fn capture(
    env: &mut JNIEnv,
    dataset: JObject,
    fragment_id: jlong,
    projection: JObject,
) -> Result<jbyteArray> {
    let fragment_id = u64::try_from(fragment_id).map_err(|_| {
        Error::input_error(format!("fragmentId must be nonnegative, got {fragment_id}"))
    })?;
    let columns = env.get_strings(&projection)?;
    let columns = columns.iter().map(String::as_str).collect::<Vec<_>>();
    let task = {
        let dataset =
            unsafe { env.get_rust_field::<_, _, BlockingDataset>(dataset, NATIVE_DATASET) }?;
        FragmentReadTask::try_new(&dataset.inner, fragment_id, &columns)?
    };
    Ok(env.byte_array_from_slice(&task.encode())?.into_raw())
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_lance_fragment_FragmentReadTask_fromFragmentNative(
    mut env: JNIEnv,
    _class: JObject,
    uri: JString,
    snapshot_version: jlong,
    schema: JByteArray,
    fragment: JByteArray,
    projection: JObject,
) -> jbyteArray {
    ok_or_throw_with_return!(
        env,
        from_fragment(
            &mut env,
            uri,
            snapshot_version,
            schema,
            fragment,
            projection,
        ),
        std::ptr::null_mut()
    )
}

fn from_fragment(
    env: &mut JNIEnv,
    uri: JString,
    snapshot_version: jlong,
    schema: JByteArray,
    fragment: JByteArray,
    projection: JObject,
) -> Result<jbyteArray> {
    let uri: String = env.get_string(&uri)?.into();
    let snapshot_version = u64::try_from(snapshot_version).map_err(|_| {
        Error::input_error(format!(
            "snapshotVersion must be nonnegative, got {snapshot_version}"
        ))
    })?;
    let schema =
        lance_file::format::pb::Schema::decode(env.convert_byte_array(&schema)?.as_slice())
            .map_err(lance_core::Error::from)?;
    let schema = Schema::try_from(FieldsWithMeta {
        fields: Fields(schema.fields),
        metadata: schema.metadata,
    })?;
    let fragment = lance_table::format::pb::DataFragment::decode(
        env.convert_byte_array(&fragment)?.as_slice(),
    )
    .map_err(lance_core::Error::from)?;
    let columns = env.get_strings(&projection)?;
    let columns = columns.iter().map(String::as_str).collect::<Vec<_>>();
    let task = FragmentReadTask::from_fragment(
        uri,
        snapshot_version,
        Arc::new(schema),
        Arc::new(Fragment::try_from(fragment)?),
        &columns,
    )?;
    Ok(env.byte_array_from_slice(&task.encode())?.into_raw())
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_lance_fragment_FragmentReadTask_validateNative(
    mut env: JNIEnv,
    _class: JObject,
    payload: JByteArray,
) {
    let result: Result<()> = (|| {
        FragmentReadTask::decode(&env.convert_byte_array(&payload)?)?;
        Ok(())
    })();
    ok_or_throw_without_return!(env, result);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_lance_fragment_FragmentReadTask_withRowAddressNative(
    mut env: JNIEnv,
    _class: JObject,
    payload: JByteArray,
) -> jbyteArray {
    let result: Result<jbyteArray> = (|| {
        let task = FragmentReadTask::decode(&env.convert_byte_array(&payload)?)?;
        Ok(env
            .byte_array_from_slice(&task.with_row_address().encode())?
            .into_raw())
    })();
    ok_or_throw_with_return!(env, result, std::ptr::null_mut())
}

/// Own the resources referenced by read futures until the Arrow consumer releases its stream.
struct PrimitiveBatchStream {
    batches: BoxStream<'static, lance_core::Result<RecordBatch>>,
    schema: SchemaRef,
    _reader: FragmentReader,
    _session: Arc<Session>,
}

impl Stream for PrimitiveBatchStream {
    type Item = lance_core::Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().batches.as_mut().poll_next(cx)
    }
}

impl RecordBatchStream for PrimitiveBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_lance_fragment_FragmentReadTask_exportNative(
    mut env: JNIEnv,
    _class: JObject,
    payload: JByteArray,
    session: JObject,
    storage_options: JObject,
    namespace: JObject,
    table_id: JObject,
    batch_size: jint,
    block_size: jint,
    offsets: JLongArray,
    stream_address: jlong,
) {
    ok_or_throw_without_return!(
        env,
        export(
            &mut env,
            payload,
            session,
            storage_options,
            namespace,
            table_id,
            batch_size,
            block_size,
            offsets,
            stream_address,
        )
    );
}

#[allow(clippy::too_many_arguments)]
fn export(
    env: &mut JNIEnv,
    payload: JByteArray,
    session: JObject,
    storage_options: JObject,
    namespace: JObject,
    table_id: JObject,
    batch_size: jint,
    block_size: jint,
    offsets: JLongArray,
    stream_address: jlong,
) -> Result<()> {
    validate_stream_address(stream_address)?;
    let batch_size = u32::try_from(batch_size)
        .ok()
        .filter(|size| *size > 0)
        .ok_or_else(|| {
            Error::input_error(format!("batchSize must be positive, got {batch_size}"))
        })?;
    let task = FragmentReadTask::decode(&env.convert_byte_array(&payload)?)?;
    let schema = task.output_schema()?;
    let (session, params) = resolve_runtime(
        env,
        &session,
        &storage_options,
        &namespace,
        &table_id,
        block_size,
    )?;
    let offsets = if offsets.is_null() {
        None
    } else {
        let mut values = vec![0; env.get_array_length(&offsets)? as usize];
        env.get_long_array_region(&offsets, 0, &mut values)?;
        let values = values
            .into_iter()
            .map(|offset| {
                u32::try_from(offset).map_err(|_| {
                    Error::input_error(format!("row offset must fit uint32, got {offset}"))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        if values.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(Error::input_error(
                "rowOffsets must be strictly increasing and unique".to_string(),
            ));
        }
        Some(values)
    };
    let stream = block_on(async {
        let reader = task
            .open_with_session(Some(params), session.clone())
            .await?;
        let batches = match offsets {
            Some(offsets) => reader.take(&offsets, batch_size, None).await?,
            None => reader.read_all(batch_size).await?,
        };
        Ok::<_, Error>(PrimitiveBatchStream {
            batches: batches.buffered(2).boxed(),
            schema,
            _reader: reader,
            _session: session,
        })
    })?;
    export_stream(stream, stream_address)
}

pub(crate) fn validate_stream_address(stream_address: jlong) -> Result<()> {
    if stream_address == 0 {
        return Err(Error::input_error(
            "ArrowArrayStream address must not be null".to_string(),
        ));
    }
    // Arrow Java buffers need not be aligned. Inspect the release callback without forming a
    // reference, and reject overwriting a live producer just as the Dataset scanner does.
    let release_is_set = unsafe {
        let ptr = stream_address as *const FFI_ArrowArrayStream;
        std::ptr::read_unaligned(std::ptr::addr_of!((*ptr).release)).is_some()
    };
    if release_is_set {
        return Err(Error::input_error(
            "ArrowArrayStream is already populated; pass a fresh, empty stream".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn resolve_runtime(
    env: &mut JNIEnv,
    session: &JObject,
    storage_options: &JObject,
    namespace: &JObject,
    table_id: &JObject,
    block_size: jint,
) -> Result<(Arc<Session>, ObjectStoreParams)> {
    let block_size = usize::try_from(block_size).map_err(|_| {
        Error::input_error(format!("blockSize must be nonnegative, got {block_size}"))
    })?;
    let session = if session.is_null() {
        Arc::new(Session::default())
    } else {
        let handle = env.get_field(session, "nativeSessionHandle", "J")?.j()?;
        session_from_handle(handle)
            .ok_or_else(|| Error::input_error("Session is closed".to_string()))?
    };
    if namespace.is_null() != table_id.is_null() {
        return Err(Error::input_error(
            "namespaceClient and tableId must be supplied together".to_string(),
        ));
    }
    let namespace = match extract_namespace_info(env, namespace, table_id) {
        Ok(namespace) => namespace,
        Err(error) => {
            // Native namespace wrappers can throw from getNativeHandle(), for example after
            // close(). Preserve that pending Java exception instead of throwing a second one.
            if env.exception_check()? {
                return Err(Error::in_exception());
            }
            return Err(error);
        }
    };
    if namespace.as_ref().is_some_and(|(_, id)| id.is_empty()) {
        return Err(Error::input_error("tableId must not be empty".to_string()));
    }
    let options_map = JMap::from_env(env, storage_options)?;
    let storage_options = to_rust_map(env, &options_map)?;
    let provider = namespace.map(|(namespace, table_id)| {
        Arc::new(LanceNamespaceStorageOptionsProvider::new(
            namespace, table_id,
        )) as Arc<dyn StorageOptionsProvider>
    });
    let accessor = match (storage_options.is_empty(), provider) {
        (false, Some(provider)) => Some(StorageOptionsAccessor::with_initial_and_provider(
            storage_options,
            provider,
        )),
        (false, None) => Some(StorageOptionsAccessor::with_static_options(storage_options)),
        (true, Some(provider)) => Some(StorageOptionsAccessor::with_provider(provider)),
        (true, None) => None,
    };
    let params = ObjectStoreParams {
        block_size: (block_size > 0).then_some(block_size),
        storage_options_accessor: accessor.map(Arc::new),
        ..Default::default()
    };
    Ok((session, params))
}

pub(crate) fn export_stream(
    stream: impl RecordBatchStream + Unpin + 'static,
    stream_address: jlong,
) -> Result<()> {
    validate_stream_address(stream_address)?;
    let ffi_stream = to_ffi_arrow_array_stream(stream, RT.handle().clone())?;
    unsafe {
        std::ptr::write_unaligned(stream_address as *mut FFI_ArrowArrayStream, ffi_stream);
    }
    Ok(())
}
