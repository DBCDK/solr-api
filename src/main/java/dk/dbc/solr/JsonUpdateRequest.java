/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GPLv3
 * See license text in LICENSE.txt
 */

package dk.dbc.solr;

import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;

import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;

public class JsonUpdateRequest extends AbstractUpdateRequest {
    private final InputStream inputStream;

    /**
     * Construct a new update request for the given InputStream.
     * @param inputStream JSON stream
     */
    public JsonUpdateRequest(InputStream inputStream) {
        super(METHOD.POST, "/update/json/docs");
        this.inputStream = inputStream;
        this.setParam("json.command", "false");
    }

    @Override
    public Collection<ContentStream> getContentStreams() {
        ContentStream jsonContentStream = new InputStreamContentStream(
                inputStream, "application/json");
        return Collections.singletonList(jsonContentStream);
    }

    /**
     * A ContentStream for wrapping an InputStream.
     */
    private class InputStreamContentStream extends ContentStreamBase {
        private final InputStream inputStream;

        public InputStreamContentStream(InputStream inputStream, String contentType) {
            this.inputStream = inputStream;
            this.setContentType(contentType);
        }

        @Override
        public InputStream getStream() {
            return inputStream;
        }
    }
}
