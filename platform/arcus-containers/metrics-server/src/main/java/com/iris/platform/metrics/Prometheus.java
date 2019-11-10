/*
 * Copyright 2019 Arcus Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.iris.platform.metrics;

import com.google.gson.JsonArray;

import com.google.inject.Inject;
import com.iris.util.ThreadPoolBuilder;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.PushGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ThreadPoolExecutor;

public class Prometheus {
   private static final Logger log = LoggerFactory.getLogger(KairosDB.class);

   private final MetricsServerConfig config;
   private final ThreadPoolExecutor executor;

   @Inject
   public Prometheus(MetricsServerConfig config) {
      this.config = config;

      this.executor = new ThreadPoolBuilder()
            .withMaxPoolSize(config.getKairosPostThreadsMax())
            .withKeepAliveMs(1000)
            .withBlockingBacklog()
            .withNameFormat("prometheus-producer-%d")
            .withMetrics("metrics-server.prometheus-producer")
            .build();

      log.info("posting metrics to prometheus pushgateway at: {}", config.getPrometheusUrl());
   }

   public void post(JsonArray metrics) {
      executor.submit(() -> {
         CollectorRegistry registry = new CollectorRegistry();

         PushGateway pg = new PushGateway(config.getPrometheusUrl());

         try {
            pg.push(registry, "job_name");
         } catch (IOException e) {
            e.printStackTrace();
         }
      });
   }
}
