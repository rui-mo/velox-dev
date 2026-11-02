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

#include "velox/type/Timestamp.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::functions::sparksql {

/// Lenient conversion of local 'time' to the GMT time.
/// If the 'time' is invalid (DST gap), it will be corrected by
/// shifting forward to the nearest valid time.
void toGMTWithGapCorrection(Timestamp& time, const tz::TimeZone& zone);

/// Converts local 'time' to the GMT time like Java's ZonedDateTime.ofLocal().
/// A nonexistent 'time' (DST gap) moves forward by the length of the gap. An
/// ambiguous 'time' (DST overlap) keeps 'preferredOffset', an offset from GMT
/// in seconds, if it is one of its two offsets, or else takes the earlier
/// instant.
void toGMTWithPreferredOffset(
    Timestamp& time,
    const tz::TimeZone& zone,
    int64_t preferredOffset);

} // namespace facebook::velox::functions::sparksql
