package com.walmart.analytics.platform.notifier.mybatis;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.text.StringSubstitutor;

import java.util.Map;

@Slf4j
public class UserGroupQueryGenerator {

    public String userGroupQuery(String queryTemplate, Map<String, Object> params) {
        log.debug("Generating query for user group from template {} using params {}",
            queryTemplate, params);
        String actualQuery = StringSubstitutor.replace(queryTemplate, params);
        log.debug("Query deparameterized: {}", actualQuery);
        return actualQuery;
    }

}
