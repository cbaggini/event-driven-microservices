package com.microservices.demo.elastic.query.service.api;

import com.microservices.demo.elastic.query.service.business.ElasticQueryService;
import com.microservices.demo.elastic.query.service.model.ElasticQueryServiceRequestModel;
import com.microservices.demo.elastic.query.service.model.ElasticQueryServiceResponseModel;
import com.microservices.demo.elastic.query.service.model.ElasticQueryServiceResponseModelV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import java.util.List;

@RestController
@RequestMapping(value = "/documents", produces = "application/vnd.api.v1+json")
public class ElasticDocumentController {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticDocumentController.class);

    private final ElasticQueryService elasticQueryService;

    public ElasticDocumentController(ElasticQueryService elasticQueryService) {
        this.elasticQueryService = elasticQueryService;
    }

    @GetMapping("/")
    public ResponseEntity<List<ElasticQueryServiceResponseModel>> getAllDocuments(){
        List<ElasticQueryServiceResponseModel> response = elasticQueryService.getAllDocuments();
        LOG.info("Elasticsearch returned {} documents", response.size());
        return ResponseEntity.ok(response);
    }

    @GetMapping("/{id}")
    public ResponseEntity<ElasticQueryServiceResponseModel> getDocumentById(@PathVariable @NotEmpty String id) {
        ElasticQueryServiceResponseModel response = elasticQueryService.getDocumentById(id);
        LOG.debug("Elasticsearch returned document with id {}", id);
        return ResponseEntity.ok(response);
    }

    @GetMapping(value = "/{id}", produces = "application/vnd.api.v2+json")
    public ResponseEntity<ElasticQueryServiceResponseModelV2> getDocumentByIdV2(@PathVariable @NotEmpty String id) {
        ElasticQueryServiceResponseModel response = elasticQueryService.getDocumentById(id);
        ElasticQueryServiceResponseModelV2 responseV2 = getV2(response);
        LOG.debug("Elasticsearch returned document with id {}", id);
        return ResponseEntity.ok(responseV2);
    }

    @PostMapping("/get-document-by-text")
    public ResponseEntity<List<ElasticQueryServiceResponseModel>> getDocumentByText(@RequestBody @Valid ElasticQueryServiceRequestModel request) {
        List<ElasticQueryServiceResponseModel> response = elasticQueryService.getDocumentByText(request.getText());
        LOG.info("Elasticsearch returned {} documents", response.size());
        return ResponseEntity.ok(response);
    }

    private ElasticQueryServiceResponseModelV2 getV2(ElasticQueryServiceResponseModel responseModel) {
        ElasticQueryServiceResponseModelV2 responseModelV2 = ElasticQueryServiceResponseModelV2.builder()
                .id(Long.parseLong(responseModel.getId()))
                .userId(responseModel.getUserId())
                .text(responseModel.getText())
                .text2("version 2 text")
                .build();
        responseModelV2.add(responseModel.getLinks());
        return responseModelV2;
    }
}
