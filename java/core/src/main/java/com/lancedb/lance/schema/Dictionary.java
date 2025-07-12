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
package com.lancedb.lance.schema;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ValueVector;

public class Dictionary {

  private final int offset;
  private final int length;
  private final ValueVector values;

  public Dictionary(int offset, int length, ValueVector values) {
    this.offset = offset;
    this.length = length;
    this.values = values;
  }

  public Dictionary(int offset, int length, DictionaryValuePointer valuePtr) {
    this.offset = offset;
    this.length = length;
    this.values = convertValuesByPtr(valuePtr);
  }

  private ValueVector convertValuesByPtr(DictionaryValuePointer valuePtr) {
    if (valuePtr == null) {
      return null;
    }
    try (RootAllocator allocator = new RootAllocator()) {
      return Data.importVector(
          allocator,
          ArrowArray.wrap(valuePtr.getArrayPtr()),
          ArrowSchema.wrap(valuePtr.getSchemaPtr()),
          null);
    }
  }

  public int getOffset() {
    return offset;
  }

  public int getLength() {
    return length;
  }

  public ValueVector getValues() {
    return values;
  }

  @Override
  public String toString() {
    return "Dictionary [offset=" + offset + ", length=" + length + ", values=" + values + "]";
  }

  public static class DictionaryValuePointer {
    private final long schemaPtr;
    private final long arrayPtr;

    public DictionaryValuePointer(long schemaPtr, long arrayPtr) {
      this.schemaPtr = schemaPtr;
      this.arrayPtr = arrayPtr;
    }

    long getSchemaPtr() {
      return schemaPtr;
    }

    long getArrayPtr() {
      return arrayPtr;
    }
  }
}
