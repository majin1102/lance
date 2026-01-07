/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lance.index;

import org.lance.Dataset;
import org.lance.Fragment;
import org.lance.TestVectorDataset;
import org.lance.Transaction;
import org.lance.index.vector.IvfBuildParams;
import org.lance.index.vector.PQBuildParams;
import org.lance.index.vector.VectorIndexParams;
import org.lance.operation.CreateIndex;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class VectorIndexTest {

  @TempDir Path tempDir;

  @Test
  public void testMergeIvfFlatIndexMetadataDistributedly() throws Exception {
    try (TestVectorDataset testVectorDataset =
        new TestVectorDataset(tempDir.resolve("merge_ivfflat_index_metadata"))) {
      try (Dataset dataset = testVectorDataset.create()) {
        List<Fragment> fragments = dataset.getFragments();
        assertTrue(
            fragments.size() >= 2,
            "Expected dataset to have at least two fragments for distributed indexing");

        int dimension = 32;
        int numPartitions = 2;

        float[] centroids = new float[numPartitions * dimension];
        for (int d = 0; d < dimension; d++) {
          centroids[d] = 0.0f;
          centroids[dimension + d] = 1.0f;
        }

        IvfBuildParams ivfParams =
            new IvfBuildParams.Builder()
                .setNumPartitions(numPartitions)
                .setMaxIters(1)
                .setCentroids(centroids)
                .build();

        VectorIndexParams vectorIndexParams =
            new VectorIndexParams.Builder(ivfParams).setDistanceType(DistanceType.L2).build();

        IndexParams indexParams =
            IndexParams.builder().setVectorIndexParams(vectorIndexParams).build();

        UUID indexUUID = UUID.randomUUID();

        // Partially create index on the first fragment
        dataset.createIndex(
            IndexOptions.builder(
                    Collections.singletonList(TestVectorDataset.vectorColumnName),
                    IndexType.IVF_FLAT,
                    indexParams)
                .withIndexName(TestVectorDataset.indexName)
                .withIndexUUID(indexUUID.toString())
                .withFragmentIds(Collections.singletonList(fragments.get(0).getId()))
                .build());

        // Partially create index on the second fragment with the same UUID
        dataset.createIndex(
            IndexOptions.builder(
                    Collections.singletonList(TestVectorDataset.vectorColumnName),
                    IndexType.IVF_FLAT,
                    indexParams)
                .withIndexName(TestVectorDataset.indexName)
                .withIndexUUID(indexUUID.toString())
                .withFragmentIds(Collections.singletonList(fragments.get(1).getId()))
                .build());

        // The index should not be visible before metadata merge & commit
        assertFalse(
            dataset.listIndexes().contains(TestVectorDataset.indexName),
            "Partially created IVF_FLAT index should not present before commit");

        // Merge index metadata for all fragment-level pieces
        dataset.mergeIndexMetadata(indexUUID.toString(), IndexType.IVF_FLAT, Optional.empty());

        int fieldId =
            dataset.getLanceSchema().fields().stream()
                .filter(f -> f.getName().equals(TestVectorDataset.vectorColumnName))
                .findAny()
                .orElseThrow(
                    () -> new RuntimeException("Cannot find vector field for TestVectorDataset"))
                .getId();

        long datasetVersion = dataset.version();

        Index index =
            Index.builder()
                .uuid(indexUUID)
                .name(TestVectorDataset.indexName)
                .fields(Collections.singletonList(fieldId))
                .datasetVersion(datasetVersion)
                .indexVersion(0)
                .fragments(
                    fragments.stream().limit(2).map(Fragment::getId).collect(Collectors.toList()))
                .build();

        CreateIndex createIndexOp =
            CreateIndex.builder().withNewIndices(Collections.singletonList(index)).build();

        Transaction createIndexTx =
            dataset.newTransactionBuilder().operation(createIndexOp).build();

        try (Dataset newDataset = createIndexTx.commit()) {
          assertEquals(datasetVersion + 1, newDataset.version());
          assertTrue(newDataset.listIndexes().contains(TestVectorDataset.indexName));
        }
      }
    }
  }

  @Test
  public void testMergeIvfPqIndexMetadataDistributedly() throws Exception {
    try (TestVectorDataset testVectorDataset =
        new TestVectorDataset(tempDir.resolve("merge_ivfpq_index_metadata"))) {
      try (Dataset dataset = testVectorDataset.create()) {
        List<Fragment> fragments = dataset.getFragments();
        assertTrue(
            fragments.size() >= 2,
            "Expected dataset to have at least two fragments for distributed indexing");

        int dimension = 32;
        int numPartitions = 2;
        int numSubVectors = 2;
        int numBits = 8;
        int numCentroids = 1 << numBits;

        float[] centroids = new float[numPartitions * dimension];
        for (int d = 0; d < dimension; d++) {
          centroids[d] = 0.0f;
          centroids[dimension + d] = 1.0f;
        }

        float[] codebook = new float[numCentroids * dimension];
        for (int c = 0; c < numCentroids; c++) {
          for (int d = 0; d < dimension; d++) {
            codebook[c * dimension + d] = (float) (c + d);
          }
        }

        IvfBuildParams ivfParams =
            new IvfBuildParams.Builder()
                .setNumPartitions(numPartitions)
                .setMaxIters(1)
                .setCentroids(centroids)
                .build();

        PQBuildParams pqParams =
            new PQBuildParams.Builder()
                .setNumSubVectors(numSubVectors)
                .setNumBits(numBits)
                .setMaxIters(2)
                .setSampleRate(256)
                .setCodebook(codebook)
                .build();

        VectorIndexParams vectorIndexParams =
            VectorIndexParams.withIvfPqParams(DistanceType.L2, ivfParams, pqParams);

        IndexParams indexParams =
            IndexParams.builder().setVectorIndexParams(vectorIndexParams).build();

        UUID indexUUID = UUID.randomUUID();

        dataset.createIndex(
            IndexOptions.builder(
                    Collections.singletonList(TestVectorDataset.vectorColumnName),
                    IndexType.IVF_PQ,
                    indexParams)
                .withIndexName(TestVectorDataset.indexName)
                .withIndexUUID(indexUUID.toString())
                .withFragmentIds(Collections.singletonList(fragments.get(0).getId()))
                .build());

        dataset.createIndex(
            IndexOptions.builder(
                    Collections.singletonList(TestVectorDataset.vectorColumnName),
                    IndexType.IVF_PQ,
                    indexParams)
                .withIndexName(TestVectorDataset.indexName)
                .withIndexUUID(indexUUID.toString())
                .withFragmentIds(Collections.singletonList(fragments.get(1).getId()))
                .build());

        assertFalse(
            dataset.listIndexes().contains(TestVectorDataset.indexName),
            "Partially created IVF_PQ index should not present before commit");

        dataset.mergeIndexMetadata(indexUUID.toString(), IndexType.IVF_PQ, Optional.empty());

        int fieldId =
            dataset.getLanceSchema().fields().stream()
                .filter(f -> f.getName().equals(TestVectorDataset.vectorColumnName))
                .findAny()
                .orElseThrow(
                    () -> new RuntimeException("Cannot find vector field for TestVectorDataset"))
                .getId();

        long datasetVersion = dataset.version();

        Index index =
            Index.builder()
                .uuid(indexUUID)
                .name(TestVectorDataset.indexName)
                .fields(Collections.singletonList(fieldId))
                .datasetVersion(datasetVersion)
                .indexVersion(0)
                .fragments(
                    fragments.stream().limit(2).map(Fragment::getId).collect(Collectors.toList()))
                .build();

        CreateIndex createIndexOp =
            CreateIndex.builder().withNewIndices(Collections.singletonList(index)).build();

        Transaction createIndexTx =
            dataset.newTransactionBuilder().operation(createIndexOp).build();

        try (Dataset newDataset = createIndexTx.commit()) {
          assertEquals(datasetVersion + 1, newDataset.version());
          assertTrue(newDataset.listIndexes().contains(TestVectorDataset.indexName));
        }
      }
    }
  }
}
