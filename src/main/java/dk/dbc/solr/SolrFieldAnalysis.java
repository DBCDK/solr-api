/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GPLv3
 * See license text in LICENSE.txt
 */

package dk.dbc.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.util.NamedList;

import java.util.ArrayList;

/**
 * A request for the org.apache.solr.handler.FieldAnalysisRequestHandler
 */
public class SolrFieldAnalysis {
    private final SolrClient solrClient;
    private final String collection;

    public SolrFieldAnalysis(SolrClient solrClient, String collection) {
        this.solrClient = solrClient;
        this.collection = collection;
    }

    /**
     * Analyze value using phases defined by given field type
     * @param fieldType field type name on which analysis should be performed
     * @param fieldValue value to be analyzed
     * @return value resulting from field analysis
     * @throws SolrServerException if unable to perform field analysis
     */
    public String byFieldType(String fieldType, String fieldValue) throws SolrServerException {
        try {
            final SolrQuery query = new SolrQuery();
            query.setRequestHandler("/analysis/field");
            query.set("analysis.fieldtype", fieldType);
            query.set("analysis.fieldvalue", fieldValue);
            return getByFieldTypeAnalysisResult(solrClient.query(collection, query), fieldType);
        } catch (Exception e) {
            throw new SolrServerException("Unable to complete field analysis request " +
                    "for type='" + fieldType + "' and value='" + fieldValue + "'", e);
        }
    }

    /*
        I would have liked to use the existing models in solrj like so:

        FieldAnalysisRequest analysisRequest = new FieldAnalysisRequest();
        analysisRequest.addFieldType("dbc-phrase");
        analysisRequest.setFieldValue("My Val");

        FieldAnalysisResponse analysisResponse = analysisRequest.process(solrClient, "my-collection");

        but this results in

        java.lang.ClassCastException: java.lang.String cannot be cast to java.util.List
	        at org.apache.solr.client.solrj.response.AnalysisResponseBase.buildPhases(AnalysisResponseBase.java:68)
	        at org.apache.solr.client.solrj.response.FieldAnalysisResponse.buildAnalysis(FieldAnalysisResponse.java:69)
	        at org.apache.solr.client.solrj.response.FieldAnalysisResponse.setResponse(FieldAnalysisResponse.java:51)
	        at org.apache.solr.client.solrj.SolrRequest.process(SolrRequest.java:160)

        in solrj versions 6.5.0 up to at least 7.2.1. This has been tested
        against solr cloud instances running both 6.5.0 and 7.2.1.

        For the newest solrj 7.5.0 version the ClassCastException seems
        to be replaced by an infinite loop.

        So either I'm trying to use the API in a way it was not intended
        or else something is simply broken inside solrj.
     */

    @SuppressWarnings("unchecked")
    private String getByFieldTypeAnalysisResult(QueryResponse queryResponse, String fieldType) {
        final NamedList<Object> response = queryResponse.getResponse();
        final NamedList<Object> analysis = getValue(response, "analysis");
        final NamedList<Object> field_types = getValue(analysis, "field_types");
        final NamedList<Object> field = getValue(field_types, fieldType);
        final NamedList<Object> index = getValue(field,"index");
        if (index.size() == 0) {
            throw new IllegalStateException("'index' element is empty");
        }
        final ArrayList<NamedList<Object>> resultPhase =
                (ArrayList<NamedList<Object>>) index.getVal(index.size() - 1);
        final String resultValue = (String) resultPhase.get(0).get("text");
        if (resultValue == null) {
            throw new IllegalStateException("Result of field analysis was null");
        }
        return resultValue;
    }

    @SuppressWarnings("unchecked")
    private NamedList<Object> getValue(NamedList<Object> container, String elementName) {
        final NamedList<Object> value = (NamedList<Object>) container.get(elementName);
        if (value == null) {
            throw new IllegalStateException("'" + elementName + "' element not found in response");
        }
        return value;
    }
}
