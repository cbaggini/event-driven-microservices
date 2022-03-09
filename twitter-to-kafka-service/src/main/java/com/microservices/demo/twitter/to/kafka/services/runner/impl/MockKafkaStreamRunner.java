package com.microservices.demo.twitter.to.kafka.services.runner.impl;

import com.microservices.demo.twitter.to.kafka.services.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.twitter.to.kafka.services.exception.TwitterToKafkaServiceException;
import com.microservices.demo.twitter.to.kafka.services.listener.TwitterKafkaStatusListener;
import com.microservices.demo.twitter.to.kafka.services.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets", havingValue = "true")
public class MockKafkaStreamRunner implements StreamRunner {

    private static final Logger LOG = LoggerFactory.getLogger(MockKafkaStreamRunner.class);

    private final TwitterKafkaStatusListener twitterKafkaStatusListener;

    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;

    private static final Random RANDOM = new Random();

    private static final String[] WORDS = new String[] {
            "lorem", "ipsum", "dolor", "sit", "amet", "consectetur",
            "adipiscing", "elit", "curabitur", "vel", "hendrerit", "libero",
            "eleifend", "blandit", "nunc", "ornare", "odio", "ut",
            "orci", "gravida", "imperdiet", "nullam", "purus", "lacinia",
            "a", "pretium", "quis", "congue", "praesent", "sagittis",
            "laoreet", "auctor", "mauris", "non", "velit", "eros",
            "dictum", "proin", "accumsan", "sapien", "nec", "massa",
            "volutpat", "venenatis", "sed", "eu", "molestie", "lacus",
            "quisque", "porttitor", "ligula", "dui", "mollis", "tempus",
            "at", "magna", "vestibulum", "turpis", "ac", "diam",
            "tincidunt", "id", "condimentum", "enim", "sodales", "in",
            "hac", "habitasse", "platea", "dictumst", "aenean", "neque",
            "fusce", "augue", "leo", "eget", "semper", "mattis",
            "tortor", "scelerisque", "nulla", "interdum", "tellus", "malesuada",
            "rhoncus", "porta", "sem", "aliquet", "et", "nam",
            "suspendisse", "potenti", "vivamus", "luctus", "fringilla", "erat",
            "donec", "justo", "vehicula", "ultricies", "varius", "ante",
            "primis", "faucibus", "ultrices", "posuere", "cubilia", "curae",
            "etiam", "cursus", "aliquam", "quam", "dapibus", "nisl",
            "feugiat", "egestas", "class", "aptent", "taciti", "sociosqu",
            "ad", "litora", "torquent", "per", "conubia", "nostra",
            "inceptos", "himenaeos", "phasellus", "nibh", "pulvinar", "vitae",
            "urna", "iaculis", "lobortis", "nisi", "viverra", "arcu",
            "morbi", "pellentesque", "metus", "commodo", "ut", "facilisis",
            "felis", "tristique", "ullamcorper", "placerat", "aenean", "convallis",
            "sollicitudin", "integer", "rutrum", "duis", "est", "etiam",
            "bibendum", "donec", "pharetra", "vulputate", "maecenas", "mi",
            "fermentum", "consequat", "suscipit", "aliquam", "habitant", "senectus",
            "netus", "fames", "quisque", "euismod", "curabitur", "lectus",
            "elementum", "tempor", "risus", "cras"};

    private static final String tweetAsRawJson = "{" +
            "\"created_at\":\"{0}\"," +
            "\"id\":\"{1}\"," +
            "\"text\":\"{2}\"," +
            "\"user\":{\"id\":\"{3}\"}" +
            "}";

    private static final String TWITTER_STATUS_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";

    public MockKafkaStreamRunner(TwitterKafkaStatusListener twitterKafkaStatusListener, TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData) {
        this.twitterKafkaStatusListener = twitterKafkaStatusListener;
        this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
    }

    @Override
    public void start() throws TwitterException {
        String[] keywords = twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);
        int minTweetLength = twitterToKafkaServiceConfigData.getMockMinTweetLength();
        int maxTweetLength = twitterToKafkaServiceConfigData.getMockMaxTweetLength();
        long sleepTimeMs = twitterToKafkaServiceConfigData.getMockSleepMs();
        LOG.info("Starting mock filtering twitter stream for keywords {}", Arrays.toString(keywords));
        simulateTwitterStream(keywords, minTweetLength, maxTweetLength, sleepTimeMs);
    }

    private void simulateTwitterStream(String[] keywords, int minTweetLength, int maxTweetLength, long sleepTimeMs) {
        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                while (true) {
                    String formattedTweetAsRawJson = getFormattedTweet(keywords, minTweetLength, maxTweetLength);
                    Status status = TwitterObjectFactory.createStatus(formattedTweetAsRawJson);
                    twitterKafkaStatusListener.onStatus(status);
                    sleep(sleepTimeMs);
                }
            } catch (TwitterException e) {
                LOG.error("Error creating twitter status", e);
            }
        });
    }

    private String getFormattedTweet(String[] keywords, int minTweetLength, int maxTweetLength) {
        String[] params = new String[] {
                ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT, Locale.ENGLISH)),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
                getRandomTweetContent(keywords, minTweetLength, maxTweetLength),
                String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE))
        };

        return formatTweetAsJsonWithParams(params);
    }

    private String formatTweetAsJsonWithParams(String[] params) {
        String tweet = tweetAsRawJson;

        for (int i = 0; i< params.length; i++) {
            tweet = tweet.replace("{" + i + "}", params[i]);
        }

        return tweet;
    }

    private String getRandomTweetContent(String[] keywords, int minTweetLength, int maxTweetLength) {
        StringBuilder tweet = new StringBuilder();
        int tweetLength = RANDOM.nextInt(maxTweetLength - minTweetLength + 1) + minTweetLength;
        return constructRandomTweet(keywords, tweet, tweetLength);
    }

    private String constructRandomTweet(String[] keywords, StringBuilder tweet, int tweetLength) {
        for (int i = 0; i< tweetLength; i++) {
            tweet.append(WORDS[RANDOM.nextInt(WORDS.length)]).append(" ");
            if (i == tweetLength / 2) {
                tweet.append(keywords[RANDOM.nextInt(keywords.length)]).append(" ");
            }
        }
        return tweet.toString().trim();
    }

    private void sleep(long sleepTimeMs) {
        try {
            Thread.sleep(sleepTimeMs);
        } catch (InterruptedException e) {
            throw new TwitterToKafkaServiceException("Error while sleeping waiting for new status to create");
        }
    }
}
