/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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
#pragma once

#include "velox/vector/BaseVector.h"

namespace facebook::velox::functions::aggregate::sparksql {

/// Replaces -0.0 with 0.0 in the REAL and DOUBLE values of 'vector', including
/// the values nested in arrays and structs. Spark aggregates that consider -0.0
/// equal to 0.0 return it as 0.0. 'vector' and the vectors nested in it must be
/// flat, like the results that aggregates extract.
void normalizeNegativeZeros(BaseVector& vector);

} // namespace facebook::velox::functions::aggregate::sparksql
