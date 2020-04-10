/*
 * Copyright (c) 2020 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include <stdlib.h>
#include <memory.h>
#include <assert.h>
#include "tbuffer.h"


////////////////////////////////////////////////////////////////////////////////
// common functions

size_t tbufTell(SBuffer* buf) {
  return buf->pos;
}

size_t tbufSeekTo(SBuffer* buf, size_t pos) {
  if (pos > buf->size) {
    // TODO: update error code, other tbufThrowError need to be changed too
    tbufThrowError(buf, 1);
  }
  size_t old = buf->pos;
  buf->pos = pos;
  return old;
}

size_t tbufSkip(SBuffer* buf, size_t size) {
  return tbufSeekTo(buf, buf->pos + size);
}

void tbufClose(SBuffer* buf, bool keepData) {
  if (!keepData) {
    (*buf->allocator)(buf->data, 0);
  }
  buf->data = NULL;
  buf->pos = 0;
  buf->size = 0;
}

////////////////////////////////////////////////////////////////////////////////
// read functions

char* tbufRead(SBuffer* buf, size_t size) {
  char* ret = buf->data + buf->pos;
  tbufSkip(buf, size);
  return ret;
}

void tbufReadToBuffer(SBuffer* buf, void* dst, size_t size) {
  assert(dst != NULL);
  // always using memcpy, leave optimization to compiler
  memcpy(dst, tbufRead(buf, size), size);
}

const char* tbufReadString(SBuffer* buf, size_t* len) {
  uint16_t l = tbufReadUint16(buf);
  char*    ret = buf->data + buf->pos;
  tbufSkip(buf, l + 1);
  ret[l] = 0;  // ensure the string end with '\0'
  if (len != NULL) {
    *len = l;
  }
  return ret;
}

size_t tbufReadToString(SBuffer* buf, char* dst, size_t size) {
  assert(dst != NULL);
  size_t      len;
  const char* str = tbufReadString(buf, &len);
  if (len >= size) {
    len = size - 1;
  }
  memcpy(dst, str, len);
  dst[len] = 0;
  return len;
}


////////////////////////////////////////////////////////////////////////////////
// write functions

void tbufEnsureCapacity(SBuffer* buf, size_t size) {
  size += buf->pos;
  if (size > buf->size) {
    size_t nsize = size + buf->size;
    char* data = (*buf->allocator)(buf->data, nsize);
    if (data == NULL) {
      tbufThrowError(buf, 2);
    }
    buf->data = data;
    buf->size = nsize;
  }
}

char* tbufGetData(SBuffer* buf, bool takeOver) {
  char* ret = buf->data;
  if (takeOver) {
    buf->pos = 0;
    buf->size = 0;
    buf->data = NULL;
  }

  return ret;
}

void tbufWrite(SBuffer* buf, const void* data, size_t size) {
  assert(data != NULL);
  tbufEnsureCapacity(buf, size);
  memcpy(buf->data + buf->pos, data, size);
  buf->pos += size;
}

void tbufWriteAt(SBuffer* buf, size_t pos, const void* data, size_t size) {
  assert(data != NULL);
  // this function can only be called to fill the gap on previous writes,
  // so 'pos + size <= buf->pos' must be true
  assert(pos + size <= buf->pos);
  memcpy(buf->data + pos, data, size);
}

void tbufWriteStringLen(SBuffer* buf, const char* str, size_t len) {
  // maximum string length is 65535, if longer string is required
  // this function and the corresponding read function need to be
  // revised.
  assert(len <= 0xffff);
  tbufWriteUint16(buf, (uint16_t)len);
  tbufWrite(buf, str, len + 1);
}

void tbufWriteString(SBuffer* buf, const char* str) {
  tbufWriteStringLen(buf, str, strlen(str));
}


////////////////////////////////////////////////////////////////////////////////
// read / write functions for primitive types

bool tbufReadBool(SBuffer* buf) {
  bool ret;
  tbufReadToBuffer(buf, &ret, sizeof(ret));
  return ret;
}

void tbufWriteBool(SBuffer* buf, bool data) {
  tbufWrite(buf, &data, sizeof(data));
}

void tbufWriteBoolAt(SBuffer* buf, size_t pos, bool data) {
  tbufWriteAt(buf, pos, &data, sizeof(data));
}

char tbufReadChar(SBuffer* buf) {
  char ret;
  tbufReadToBuffer(buf, &ret, sizeof(ret));
  return ret;
}

void tbufWriteChar(SBuffer* buf, char data) {
  tbufWrite(buf, &data, sizeof(data));
}

void tbufWriteCharAt(SBuffer* buf, size_t pos, char data) {
  tbufWriteAt(buf, pos, &data, sizeof(data));
}

int8_t tbufReadInt8(SBuffer* buf) {
  int8_t ret;
  tbufReadToBuffer(buf, &ret, sizeof(ret));
  return ret;
}

void tbufWriteInt8(SBuffer* buf, int8_t data) {
  tbufWrite(buf, &data, sizeof(data));
}

void tbufWriteInt8At(SBuffer* buf, size_t pos, int8_t data) {
  tbufWriteAt(buf, pos, &data, sizeof(data));
}

uint8_t tbufReadUint8(SBuffer* buf) {
  uint8_t ret;
  tbufReadToBuffer(buf, &ret, sizeof(ret));
  return ret;
}

void tbufWriteUint8(SBuffer* buf, uint8_t data) {
  tbufWrite(buf, &data, sizeof(data));
}

void tbufWriteUint8At(SBuffer* buf, size_t pos, uint8_t data) {
  tbufWriteAt(buf, pos, &data, sizeof(data));
}

int16_t tbufReadInt16(SBuffer* buf) {
  int16_t ret;
  tbufReadToBuffer(buf, &ret, sizeof(ret));
  if (buf->network) {
    return (int16_t)ntohs(ret);
  }
  return ret;
}

void tbufWriteInt16(SBuffer* buf, int16_t data) {
  if (buf->network) {
    data = (int16_t)htons(data);
  }
  tbufWrite(buf, &data, sizeof(data));
}

void tbufWriteInt16At(SBuffer* buf, size_t pos, int16_t data) {
  if (buf->network) {
    data = (int16_t)htons(data);
  }
  tbufWriteAt(buf, pos, &data, sizeof(data));
}

uint16_t tbufReadUint16(SBuffer* buf) {
  uint16_t ret;
  tbufReadToBuffer(buf, &ret, sizeof(ret));
  if (buf->network) {
    return ntohs(ret);
  }
  return ret;
}

void tbufWriteUint16(SBuffer* buf, uint16_t data) {
  if (buf->network) {
    data = htons(data);
  }
  tbufWrite(buf, &data, sizeof(data));
}

void tbufWriteUint16At(SBuffer* buf, size_t pos, uint16_t data) {
  if (buf->network) {
    data = htons(data);
  }
  tbufWriteAt(buf, pos, &data, sizeof(data));
}

int32_t tbufReadInt32(SBuffer* buf) {
  int32_t ret;
  tbufReadToBuffer(buf, &ret, sizeof(ret));
  if (buf->network) {
    return (int32_t)ntohl(ret);
  }
  return ret;
}

void tbufWriteInt32(SBuffer* buf, int32_t data) {
  if (buf->network) {
    data = (int32_t)htonl(data);
  }
  tbufWrite(buf, &data, sizeof(data));
}

void tbufWriteInt32At(SBuffer* buf, size_t pos, int32_t data) {
  if (buf->network) {
    data = (int32_t)htonl(data);
  }
  tbufWriteAt(buf, pos, &data, sizeof(data));
}

uint32_t tbufReadUint32(SBuffer* buf) {
  uint32_t ret;
  tbufReadToBuffer(buf, &ret, sizeof(ret));
  if (buf->network) {
    return ntohl(ret);
  }
  return ret;
}

void tbufWriteUint32(SBuffer* buf, uint32_t data) {
  if (buf->network) {
    data = htonl(data);
  }
  tbufWrite(buf, &data, sizeof(data));
}

void tbufWriteUint32At(SBuffer* buf, size_t pos, uint32_t data) {
  if (buf->network) {
    data = htonl(data);
  }
  tbufWriteAt(buf, pos, &data, sizeof(data));
}

int64_t tbufReadInt64(SBuffer* buf) {
  int64_t ret;
  tbufReadToBuffer(buf, &ret, sizeof(ret));
  if (buf->network) {
    return (int64_t)htobe64(ret); // TODO: ntohll
  }
  return ret;
}

void tbufWriteInt64(SBuffer* buf, int64_t data) {
  if (buf->network) {
    data = (int64_t)htobe64(data);
  }
  tbufWrite(buf, &data, sizeof(data));
}

void tbufWriteInt64At(SBuffer* buf, size_t pos, int64_t data) {
  if (buf->network) {
    data = (int64_t)htobe64(data);
  }
  tbufWriteAt(buf, pos, &data, sizeof(data));
}

uint64_t tbufReadUint64(SBuffer* buf) {
  uint64_t ret;
  tbufReadToBuffer(buf, &ret, sizeof(ret));
  if (buf->network) {
    return htobe64(ret); // TODO: ntohll
  }
  return ret;
}

void tbufWriteUint64(SBuffer* buf, uint64_t data) {
  if (buf->network) {
    data = htobe64(data);
  }
  tbufWrite(buf, &data, sizeof(data));
}

void tbufWriteUint64At(SBuffer* buf, size_t pos, uint64_t data) {
  if (buf->network) {
    data = htobe64(data);
  }
  tbufWriteAt(buf, pos, &data, sizeof(data));
}

float tbufReadFloat(SBuffer* buf) {
  uint32_t ret = tbufReadUint32(buf);
  return *(float*)(&ret);
}

void tbufWriteFloat(SBuffer* buf, float data) {
  tbufWriteUint32(buf, *(uint32_t*)(&data));
}

void tbufWriteFloatAt(SBuffer* buf, size_t pos, float data) {
  tbufWriteUint32At(buf, pos, *(uint32_t*)(&data));
}

double tbufReadDouble(SBuffer* buf) {
  uint64_t ret = tbufReadUint64(buf);
  return *(double*)(&ret);
}

void tbufWriteDouble(SBuffer* buf, double data) {
  tbufWriteUint64(buf, *(uint64_t*)(&data));
}

void tbufWriteDoubleAt(SBuffer* buf, size_t pos, double data) {
  tbufWriteUint64At(buf, pos, *(uint64_t*)(&data));
}
