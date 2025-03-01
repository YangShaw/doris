// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "parquet_common.h"

#include "util/coding.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {

const cctz::time_zone DecodeParams::utc0 = cctz::utc_time_zone();

const uint32_t ParquetInt96::JULIAN_EPOCH_OFFSET_DAYS = 2440588;
const uint64_t ParquetInt96::MICROS_IN_DAY = 86400000000;
const uint64_t ParquetInt96::NANOS_PER_MICROSECOND = 1000;

inline uint64_t ParquetInt96::to_timestamp_micros() const {
    return (hi - JULIAN_EPOCH_OFFSET_DAYS) * MICROS_IN_DAY + lo / NANOS_PER_MICROSECOND;
}

#define FOR_LOGICAL_NUMERIC_TYPES(M) \
    M(TypeIndex::Int32, Int32)       \
    M(TypeIndex::UInt32, UInt32)     \
    M(TypeIndex::Int64, Int64)       \
    M(TypeIndex::UInt64, UInt64)     \
    M(TypeIndex::Float32, Float32)   \
    M(TypeIndex::Float64, Float64)

Status Decoder::get_decoder(tparquet::Type::type type, tparquet::Encoding::type encoding,
                            std::unique_ptr<Decoder>& decoder) {
    switch (encoding) {
    case tparquet::Encoding::PLAIN:
        switch (type) {
        case tparquet::Type::BOOLEAN:
            decoder.reset(new BoolPlainDecoder());
            break;
        case tparquet::Type::BYTE_ARRAY:
            decoder.reset(new ByteArrayPlainDecoder());
            break;
        case tparquet::Type::INT32:
        case tparquet::Type::INT64:
        case tparquet::Type::INT96:
        case tparquet::Type::FLOAT:
        case tparquet::Type::DOUBLE:
        case tparquet::Type::FIXED_LEN_BYTE_ARRAY:
            decoder.reset(new PlainDecoder(type));
            break;
        default:
            return Status::InternalError("Unsupported plain type {} in parquet decoder",
                                         tparquet::to_string(type));
        }
    case tparquet::Encoding::RLE_DICTIONARY:
        break;
    default:
        return Status::InternalError("Unsupported encoding {} in parquet decoder",
                                     tparquet::to_string(encoding));
    }
    return Status::OK();
}

void Decoder::init(FieldSchema* field_schema, cctz::time_zone* ctz) {
    _field_schema = field_schema;
    if (_decode_params == nullptr) {
        _decode_params.reset(new DecodeParams());
    }
    if (ctz != nullptr) {
        _decode_params->ctz = ctz;
    }
    const auto& schema = field_schema->parquet_schema;
    if (schema.__isset.logicalType && schema.logicalType.__isset.TIMESTAMP) {
        const auto& timestamp_info = schema.logicalType.TIMESTAMP;
        if (!timestamp_info.isAdjustedToUTC) {
            // should set timezone to utc+0
            _decode_params->ctz = const_cast<cctz::time_zone*>(&_decode_params->utc0);
        }
        const auto& time_unit = timestamp_info.unit;
        if (time_unit.__isset.MILLIS) {
            _decode_params->second_mask = 1000;
            _decode_params->scale_to_nano_factor = 1000000;
        } else if (time_unit.__isset.MICROS) {
            _decode_params->second_mask = 1000000;
            _decode_params->scale_to_nano_factor = 1000;
        } else if (time_unit.__isset.NANOS) {
            _decode_params->second_mask = 1000000000;
            _decode_params->scale_to_nano_factor = 1;
        }
    } else if (schema.__isset.converted_type) {
        const auto& converted_type = schema.converted_type;
        if (converted_type == tparquet::ConvertedType::TIMESTAMP_MILLIS) {
            _decode_params->second_mask = 1000;
            _decode_params->scale_to_nano_factor = 1000000;
        } else if (converted_type == tparquet::ConvertedType::TIMESTAMP_MICROS) {
            _decode_params->second_mask = 1000000;
            _decode_params->scale_to_nano_factor = 1000;
        }
    }
}

Status Decoder::decode_values(ColumnPtr& doris_column, DataTypePtr& data_type, size_t num_values) {
    CHECK(doris_column->is_nullable());
    auto* nullable_column = reinterpret_cast<vectorized::ColumnNullable*>(
            (*std::move(doris_column)).mutate().get());
    MutableColumnPtr data_column = nullable_column->get_nested_column_ptr();
    return decode_values(data_column, data_type, num_values);
}

Status PlainDecoder::decode_values(Slice& slice, size_t num_values) {
    size_t to_read_bytes = _type_length * num_values;
    if (UNLIKELY(to_read_bytes > slice.size)) {
        return Status::IOError("Slice does not have enough space to write out the decoding data");
    }
    memcpy(slice.data, _data->data + _offset, to_read_bytes);
    _offset += to_read_bytes;
    return Status::OK();
}

Status PlainDecoder::skip_values(size_t num_values) {
    _offset += _type_length * num_values;
    if (UNLIKELY(_offset > _data->size)) {
        return Status::IOError("Out-of-bounds access in parquet data decoder");
    }
    return Status::OK();
}

Status PlainDecoder::_decode_short_int(MutableColumnPtr& doris_column, size_t num_values,
                                       size_t real_length) {
    if (UNLIKELY(_physical_type != tparquet::Type::INT32)) {
        return Status::InternalError("Short int can only be decoded from INT32");
    }
    for (int i = 0; i < num_values; ++i) {
        doris_column->insert_data(_data->data + _offset, real_length);
        _offset += _type_length;
    }
    return Status::OK();
}

Status PlainDecoder::decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                   size_t num_values) {
    if (UNLIKELY(_offset + _type_length * num_values > _data->size)) {
        return Status::IOError("Out-of-bounds access in parquet data decoder");
    }
    TypeIndex logical_type = remove_nullable(data_type)->get_type_id();
    switch (logical_type) {
    case TypeIndex::Int8:
    case TypeIndex::UInt8:
        return _decode_short_int(doris_column, num_values, 1);
    case TypeIndex::Int16:
    case TypeIndex::UInt16:
        return _decode_short_int(doris_column, num_values, 2);
#define DISPATCH(NUMERIC_TYPE, CPP_NUMERIC_TYPE) \
    case NUMERIC_TYPE:                           \
        return _decode_numeric<CPP_NUMERIC_TYPE>(doris_column, num_values);
        FOR_LOGICAL_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    case TypeIndex::Date:
        if (_physical_type == tparquet::Type::INT32) {
            return _decode_date<VecDateTimeValue, Int64>(doris_column, logical_type, num_values);
        }
        break;
    case TypeIndex::DateV2:
        if (_physical_type == tparquet::Type::INT32) {
            return _decode_date<DateV2Value<DateV2ValueType>, UInt32>(doris_column, logical_type,
                                                                      num_values);
        }
        break;
    case TypeIndex::DateTime:
        if (_physical_type == tparquet::Type::INT96) {
            return _decode_datetime96<VecDateTimeValue, Int64>(doris_column, logical_type,
                                                               num_values);
        } else if (_physical_type == tparquet::Type::INT64) {
            return _decode_datetime64<VecDateTimeValue, Int64>(doris_column, logical_type,
                                                               num_values);
        }
        break;
    case TypeIndex::DateTimeV2:
        // Spark can set the timestamp precision by the following configuration:
        // spark.sql.parquet.outputTimestampType = INT96(NANOS), TIMESTAMP_MICROS, TIMESTAMP_MILLIS
        if (_physical_type == tparquet::Type::INT96) {
            return _decode_datetime96<DateV2Value<DateTimeV2ValueType>, UInt64>(
                    doris_column, logical_type, num_values);
        } else if (_physical_type == tparquet::Type::INT64) {
            return _decode_datetime64<DateV2Value<DateTimeV2ValueType>, UInt64>(
                    doris_column, logical_type, num_values);
        }
        break;
    case TypeIndex::Decimal32:
        if (_physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
            return _decode_binary_decimal<Int32>(doris_column, data_type, num_values);
        } else if (_physical_type == tparquet::Type::INT32) {
            return _decode_primitive_decimal<Int32, Int32>(doris_column, data_type, num_values);
        } else if (_physical_type == tparquet::Type::INT64) {
            return _decode_primitive_decimal<Int32, Int64>(doris_column, data_type, num_values);
        }
        break;
    case TypeIndex::Decimal64:
        if (_physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
            return _decode_binary_decimal<Int64>(doris_column, data_type, num_values);
        } else if (_physical_type == tparquet::Type::INT32) {
            return _decode_primitive_decimal<Int64, Int32>(doris_column, data_type, num_values);
        } else if (_physical_type == tparquet::Type::INT64) {
            return _decode_primitive_decimal<Int64, Int64>(doris_column, data_type, num_values);
        }
        break;
    case TypeIndex::Decimal128:
        if (_physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
            return _decode_binary_decimal<Int128>(doris_column, data_type, num_values);
        } else if (_physical_type == tparquet::Type::INT32) {
            return _decode_primitive_decimal<Int128, Int32>(doris_column, data_type, num_values);
        } else if (_physical_type == tparquet::Type::INT64) {
            return _decode_primitive_decimal<Int128, Int64>(doris_column, data_type, num_values);
        }
        break;
    case TypeIndex::String:
    case TypeIndex::FixedString:
        if (_physical_type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
            for (int i = 0; i < num_values; ++i) {
                doris_column->insert_data(_data->data + _offset, _type_length);
                _offset += _type_length;
            }
            return Status::OK();
        }
        break;
    default:
        break;
    }

    return Status::InvalidArgument("Can't decode parquet physical type {} to doris logical type {}",
                                   tparquet::to_string(_physical_type),
                                   getTypeName(data_type->get_type_id()));
}

Status ByteArrayPlainDecoder::decode_values(Slice& slice, size_t num_values) {
    uint32_t slice_offset = 0;
    for (int i = 0; i < num_values; ++i) {
        if (UNLIKELY(_offset + 4 > _data->size)) {
            return Status::IOError("Can't read byte array length from plain decoder");
        }
        uint32_t length =
                decode_fixed32_le(reinterpret_cast<const uint8_t*>(_data->data) + _offset);
        _offset += 4;
        if (UNLIKELY(_offset + length) > _data->size) {
            return Status::IOError("Can't read enough bytes in plain decoder");
        }
        memcpy(slice.data + slice_offset, _data->data + _offset, length);
        slice_offset += length + 1;
        slice.data[slice_offset - 1] = '\0';
        _offset += length;
    }
    return Status::OK();
}

Status ByteArrayPlainDecoder::skip_values(size_t num_values) {
    for (int i = 0; i < num_values; ++i) {
        if (UNLIKELY(_offset + 4 > _data->size)) {
            return Status::IOError("Can't read byte array length from plain decoder");
        }
        uint32_t length =
                decode_fixed32_le(reinterpret_cast<const uint8_t*>(_data->data) + _offset);
        _offset += 4;
        if (UNLIKELY(_offset + length) > _data->size) {
            return Status::IOError("Can't skip enough bytes in plain decoder");
        }
        _offset += length;
    }
    return Status::OK();
}

Status ByteArrayPlainDecoder::decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                            size_t num_values) {
    TypeIndex logical_type = remove_nullable(data_type)->get_type_id();
    switch (logical_type) {
    case TypeIndex::String:
    case TypeIndex::FixedString:
        for (int i = 0; i < num_values; ++i) {
            if (UNLIKELY(_offset + 4 > _data->size)) {
                return Status::IOError("Can't read byte array length from plain decoder");
            }
            uint32_t length =
                    decode_fixed32_le(reinterpret_cast<const uint8_t*>(_data->data) + _offset);
            _offset += 4;
            if (UNLIKELY(_offset + length) > _data->size) {
                return Status::IOError("Can't read enough bytes in plain decoder");
            }
            doris_column->insert_data(_data->data + _offset, length);
            _offset += length;
        }
        return Status::OK();
    case TypeIndex::Decimal32:
        return _decode_binary_decimal<Int32>(doris_column, data_type, num_values);
    case TypeIndex::Decimal64:
        return _decode_binary_decimal<Int64>(doris_column, data_type, num_values);
    case TypeIndex::Decimal128:
        return _decode_binary_decimal<Int128>(doris_column, data_type, num_values);
    default:
        break;
    }
    return Status::InvalidArgument(
            "Can't decode parquet physical type BYTE_ARRAY to doris logical type {}",
            getTypeName(data_type->get_type_id()));
}

Status BoolPlainDecoder::decode_values(Slice& slice, size_t num_values) {
    bool value;
    for (int i = 0; i < num_values; ++i) {
        if (UNLIKELY(!_decode_value(&value))) {
            return Status::IOError("Can't read enough booleans in plain decoder");
        }
        slice.data[i] = value ? 1 : 0;
    }
    return Status::OK();
}

Status BoolPlainDecoder::skip_values(size_t num_values) {
    int skip_cached = std::min(num_unpacked_values_ - unpacked_value_idx_, (int)num_values);
    unpacked_value_idx_ += skip_cached;
    if (skip_cached == num_values) {
        return Status::OK();
    }
    int num_remaining = num_values - skip_cached;
    int num_to_skip = BitUtil::RoundDownToPowerOf2(num_remaining, 32);
    if (num_to_skip > 0) {
        bool_values_.SkipBatch(1, num_to_skip);
    }
    num_remaining -= num_to_skip;
    if (num_remaining > 0) {
        DCHECK_LE(num_remaining, UNPACKED_BUFFER_LEN);
        num_unpacked_values_ =
                bool_values_.UnpackBatch(1, UNPACKED_BUFFER_LEN, &unpacked_values_[0]);
        if (UNLIKELY(num_unpacked_values_ < num_remaining)) {
            return Status::IOError("Can't skip enough booleans in plain decoder");
        }
        unpacked_value_idx_ = num_remaining;
    }
    return Status::OK();
}

Status BoolPlainDecoder::decode_values(MutableColumnPtr& doris_column, DataTypePtr& data_type,
                                       size_t num_values) {
    auto& column_data = static_cast<ColumnVector<UInt8>&>(*doris_column).get_data();
    bool value;
    for (int i = 0; i < num_values; ++i) {
        if (UNLIKELY(!_decode_value(&value))) {
            return Status::IOError("Can't read enough booleans in plain decoder");
        }
        column_data.emplace_back(value);
    }
    return Status::OK();
}
} // namespace doris::vectorized
