package com.microservices.demo.elastic.query.client.service.impl;

import com.microservices.demo.common.util.CollectionsUtil;
import com.microservices.demo.elastic.model.index.impl.TwitterIndexModel;
import com.microservices.demo.elastic.query.client.exception.ElasticQueryClientException;
import com.microservices.demo.elastic.query.client.repository.TwitterElasticQueryRepository;
import com.microservices.demo.elastic.query.client.service.ElasticQueryClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
@Primary
public class TwitterElasticRepositoryQueryClient implements ElasticQueryClient<TwitterIndexModel> {
    private static final Logger LOG = LoggerFactory.getLogger(TwitterElasticRepositoryQueryClient.class);

    private final TwitterElasticQueryRepository twitterElasticQueryRepository;

    public TwitterElasticRepositoryQueryClient(TwitterElasticQueryRepository twitterElasticQueryRepository) {
        this.twitterElasticQueryRepository = twitterElasticQueryRepository;
    }

    @Override
    public TwitterIndexModel getIndexModelById(String id) {
        Optional<TwitterIndexModel> searchResult = twitterElasticQueryRepository.findById(id);
        LOG.info("Document with id {} retrieved successfully",
                searchResult.orElseThrow(() ->
                        new ElasticQueryClientException("No document found with id " + id)).getId());
        return searchResult.get();
    }

    @Override
    public List<TwitterIndexModel> getIndexModelByText(String text) {
        List<TwitterIndexModel> searchResults = twitterElasticQueryRepository.findByText(text);
        LOG.info("{} of documents with text {} retrieved successfully", searchResults.size(), text);
        return searchResults;
    }

    @Override
    public List<TwitterIndexModel> getAllIndexModel() {
        List<TwitterIndexModel> searchResults = CollectionsUtil.getInstance().getListFromIterable(twitterElasticQueryRepository.findAll());
        LOG.info("{} of documents retrieved successfully", searchResults.size());
        return searchResults;
    }
}
