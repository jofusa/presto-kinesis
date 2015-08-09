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
import com.facebook.presto.kinesis.decoder.KinesisRowDecoder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.google.common.base.Splitter;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.InflaterInputStream;

import static com.google.common.base.Preconditions.checkState;

public class Base64ZLibJsonKinesisRowDecoder
        implements KinesisRowDecoder
{
    public static final String NAME = "base64-zlib-json";
    private final ObjectMapper objectMapper;

    @Inject
    public Base64ZLibJsonKinesisRowDecoder(ObjectMapper objectMapper)
    {
        this.objectMapper = objectMapper;
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public boolean decodeRow(byte[] data, Set<KinesisFieldValueProvider> fieldValueProviders, List<KinesisColumnHandle> columnHandles, Map<KinesisColumnHandle, KinesisFieldDecoder<?>> fieldDecoders)
    {
        byte[] decodedData;
        JsonNode tree;

        try {
            decodedData = decodeAndUncompress(data);
        }
        catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        try {
            tree = objectMapper.readTree(decodedData);
        }
        catch (Exception e) {
            return false;
        }

        for (KinesisColumnHandle columnHandle : columnHandles) {
            if (columnHandle.isInternal()) {
                continue;
            }
            @SuppressWarnings("unchecked")
            KinesisFieldDecoder<JsonNode> decoder = (KinesisFieldDecoder<JsonNode>) fieldDecoders.get(columnHandle);

            if (decoder != null) {
                JsonNode node = locateNode(tree, columnHandle);
                fieldValueProviders.add(decoder.decode(node, columnHandle));
            }
        }
        return true;
    }

    private static JsonNode locateNode(JsonNode tree, KinesisColumnHandle columnHandle)
    {
        String mapping = columnHandle.getMapping();
        checkState(mapping != null, "No mapping for %s", columnHandle.getName());

        JsonNode currentNode = tree;
        for (String pathElement : Splitter.on('/').omitEmptyStrings().split(mapping)) {
            if (!currentNode.has(pathElement)) {
                return MissingNode.getInstance();
            }
            currentNode = currentNode.path(pathElement);
        }
        return currentNode;
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
