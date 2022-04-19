package com.microservices.demo.elastic.query.service.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotEmpty;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ElasticQueryServiceRequestModel {
    private String id;

    @NotEmpty
    private String text;
}
