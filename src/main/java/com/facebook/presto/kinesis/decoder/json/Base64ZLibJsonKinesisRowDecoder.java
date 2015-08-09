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
package com.facebook.presto.kinesis.decoder.json;

import com.facebook.presto.kinesis.KinesisColumnHandle;
import com.facebook.presto.kinesis.KinesisFieldValueProvider;
import com.facebook.presto.kinesis.decoder.KinesisFieldDecoder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.InflaterInputStream;

public class Base64ZLibJsonKinesisRowDecoder
        extends JsonKinesisRowDecoder
{
    public static final String NAME = "base64-zlib-json";

    public Base64ZLibJsonKinesisRowDecoder(ObjectMapper objectMapper) {
        super(objectMapper);
    }

    @Override
    public boolean decodeRow(byte[] data, Set<KinesisFieldValueProvider> fieldValueProviders, List<KinesisColumnHandle> columnHandles, Map<KinesisColumnHandle, KinesisFieldDecoder<?>> fieldDecoders)
    {
        // convert data from a base64'd zlib format to json
        byte[] decodedData;
        try {
            decodedData = decodeAndUncompress(data);
        }
        catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return super.decodeRow(decodedData, fieldValueProviders, columnHandles, fieldDecoders);
    }

    /**
     * Decodes and decompressed a string of information that's encoded via base 64 and compressed via zlib
     */
    public static byte[] decodeAndUncompress(byte[] data) throws IOException
    {
        String stringData = new String(data, "UTF-8");
        BaseEncoding decoder = BaseEncoding.base64().withSeparator("\n", 1);
        ByteArrayInputStream compressed = new ByteArrayInputStream(decoder.decode(stringData));
        InflaterInputStream decompressed = new InflaterInputStream(compressed);

        return ByteStreams.toByteArray(decompressed);
    }
}
