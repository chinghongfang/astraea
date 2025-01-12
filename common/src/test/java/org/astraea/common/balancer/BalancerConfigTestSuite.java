/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.common.balancer;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.admin.ClusterInfo;
import org.astraea.common.cost.ClusterCost;
import org.astraea.common.cost.HasClusterCost;
import org.astraea.common.metrics.ClusterBean;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** A collection of helper methods that aid in verifying the implementation of balancer configs. */
public abstract class BalancerConfigTestSuite {

  private final Class<? extends Balancer> balancerClass;
  private final Configuration customConfig;

  public BalancerConfigTestSuite(Class<? extends Balancer> balancerClass, Configuration custom) {
    this.balancerClass = balancerClass;
    this.customConfig = custom;
  }

  @Test
  public void testBalancerAllowedTopicsRegex() {
    final var balancer = Utils.construct(balancerClass, Configuration.EMPTY);
    final var cluster = cluster(20, 10, 10, (short) 5);

    {
      var testName = "[test no limit]";
      var plan =
          balancer.offer(
              AlgorithmConfig.builder()
                  .clusterInfo(cluster)
                  .clusterCost(decreasingCost())
                  .timeout(Duration.ofSeconds(2))
                  .configs(customConfig.raw())
                  // This argument is not applied
                  // .config(BalancerConfigs.BALANCER_ALLOWED_TOPICS_REGEX, regexRaw)
                  .build());
      AssertionsHelper.assertSomeMovement(cluster, plan.orElseThrow().proposal(), testName);
    }

    {
      var testName = "[test only allowed topics being altered]";
      var regexRaw =
          cluster.topicNames().stream()
              .limit(5)
              .map(Pattern::quote)
              .collect(Collectors.joining("|", "(", ")"));
      var regex = Pattern.compile(regexRaw);
      var plan =
          balancer.offer(
              AlgorithmConfig.builder()
                  .clusterInfo(cluster)
                  .clusterCost(decreasingCost())
                  .timeout(Duration.ofSeconds(2))
                  .configs(customConfig.raw())
                  .config(BalancerConfigs.BALANCER_ALLOWED_TOPICS_REGEX, regexRaw)
                  .build());
      AssertionsHelper.assertOnlyAllowedTopicMovement(
          cluster, plan.orElseThrow().proposal(), regex, testName);
    }

    {
      var testName = "[test the regex should match the whole topic name]";
      var regexRaw =
          cluster.topicNames().stream()
              .limit(5)
              .map(name -> name.substring(0, 1))
              .map(Pattern::quote)
              .collect(Collectors.joining("|", "(", ")"));
      var plan =
          balancer.offer(
              AlgorithmConfig.builder()
                  .clusterInfo(cluster)
                  .clusterCost(decreasingCost())
                  .timeout(Duration.ofSeconds(2))
                  .configs(customConfig.raw())
                  .config(BalancerConfigs.BALANCER_ALLOWED_TOPICS_REGEX, regexRaw)
                  .build());
      AssertionsHelper.assertNoMovement(cluster, plan.orElseThrow().proposal(), testName);
    }

    {
      var testName = "[test no allowed topic should generate no plan]";
      var regexRaw = "";
      var plan =
          balancer.offer(
              AlgorithmConfig.builder()
                  .clusterInfo(cluster)
                  .clusterCost(decreasingCost())
                  .timeout(Duration.ofSeconds(2))
                  .configs(customConfig.raw())
                  .config(BalancerConfigs.BALANCER_ALLOWED_TOPICS_REGEX, regexRaw)
                  .build());
      AssertionsHelper.assertNoMovement(cluster, plan.orElseThrow().proposal(), testName);
    }
  }

  @Test
  public void testBalancerAllowedBrokersRegex() {
    final var balancer = Utils.construct(balancerClass, Configuration.EMPTY);
    final var cluster = cluster(10, 10, 10, (short) 5);

    {
      var testName = "[test all match]";
      var plan =
          balancer.offer(
              AlgorithmConfig.builder()
                  .clusterInfo(cluster)
                  .clusterCost(decreasingCost())
                  .timeout(Duration.ofSeconds(2))
                  .configs(customConfig.raw())
                  .config(BalancerConfigs.BALANCER_ALLOWED_BROKERS_REGEX, "[0-9]*")
                  .build());
      AssertionsHelper.assertSomeMovement(cluster, plan.orElseThrow().proposal(), testName);
    }

    {
      var testName = "[test no match]";
      var plan =
          balancer.offer(
              AlgorithmConfig.builder()
                  .clusterInfo(cluster)
                  .clusterCost(decreasingCost())
                  .timeout(Duration.ofSeconds(2))
                  .configs(customConfig.raw())
                  .config(BalancerConfigs.BALANCER_ALLOWED_BROKERS_REGEX, "NoMatch")
                  .build());
      // since nothing can be moved. It is ok to return no plan.
      if (plan.isPresent()) {
        // But if we have a plan here. It must contain no movement.
        AssertionsHelper.assertNoMovement(cluster, plan.orElseThrow().proposal(), testName);
      }
    }

    {
      var testName = "[test some match]";
      var allowedBrokers = IntStream.range(1, 6).boxed().collect(Collectors.toUnmodifiableSet());
      var rawRegex =
          allowedBrokers.stream().map(Object::toString).collect(Collectors.joining("|", "(", ")"));
      var plan =
          balancer.offer(
              AlgorithmConfig.builder()
                  .clusterInfo(cluster)
                  .clusterCost(decreasingCost())
                  .timeout(Duration.ofSeconds(2))
                  .configs(customConfig.raw())
                  .config(BalancerConfigs.BALANCER_ALLOWED_BROKERS_REGEX, rawRegex)
                  .build());
      AssertionsHelper.assertOnlyAllowedBrokerMovement(
          cluster, plan.orElseThrow().proposal(), allowedBrokers::contains, testName);
    }
  }

  private static ClusterInfo cluster(int nodes, int topics, int partitions, short replicas) {
    var builder =
        ClusterInfo.builder()
            .addNode(IntStream.range(0, nodes).boxed().collect(Collectors.toSet()))
            .addFolders(
                IntStream.range(0, nodes)
                    .boxed()
                    .collect(
                        Collectors.toMap(
                            id -> id, id -> Set.of("/folder0", "/folder1", "/folder2"))));
    for (int i = 0; i < topics; i++)
      builder = builder.addTopic(Utils.randomString(), partitions, replicas);
    return builder.build();
  }

  private static HasClusterCost decreasingCost() {
    return new HasClusterCost() {

      private final AtomicReference<ClusterInfo> initial = new AtomicReference<>();
      private final AtomicReference<Double> score = new AtomicReference<>(1.0);

      @Override
      public ClusterCost clusterCost(ClusterInfo clusterInfo, ClusterBean clusterBean) {
        if (initial.get() == null) initial.set(clusterInfo);
        return ClusterCost.of(
            clusterInfo == initial.get() ? 1 : score.updateAndGet(i -> i * 0.999999),
            () -> "DecreasingCost");
      }
    };
  }

  private static class AssertionsHelper {
    static void assertSomeMovement(ClusterInfo source, ClusterInfo target, String name) {
      Assertions.assertNotEquals(
          Set.of(),
          ClusterInfo.findNonFulfilledAllocation(source, target),
          name + ": Should have movements");
    }

    static void assertNoMovement(ClusterInfo source, ClusterInfo target, String name) {
      Assertions.assertEquals(
          Set.of(),
          ClusterInfo.findNonFulfilledAllocation(source, target),
          name + ": Should have no movement");
    }

    static void assertOnlyAllowedTopicMovement(
        ClusterInfo source, ClusterInfo target, Pattern allowedTopic, String name) {
      assertSomeMovement(source, target, name);
      Assertions.assertEquals(
          Set.of(),
          ClusterInfo.findNonFulfilledAllocation(source, target).stream()
              .filter(Predicate.not((tp) -> allowedTopic.asMatchPredicate().test(tp.topic())))
              .collect(Collectors.toUnmodifiableSet()),
          name + ": Only allowed topics been altered.");
    }

    static void assertOnlyAllowedBrokerMovement(
        ClusterInfo source, ClusterInfo target, Predicate<Integer> allowedBroker, String name) {
      assertSomeMovement(source, target, name);
      source
          .replicaStream()
          // for those replicas that are not allowed to move
          .filter(r -> !allowedBroker.test(r.nodeInfo().id()))
          // they should exist as-is in the target allocation
          .forEach(
              fixedReplica -> {
                target
                    .replicaStream()
                    .filter(targetReplica -> targetReplica.equals(fixedReplica))
                    .findFirst()
                    .ifPresentOrElse(
                        (r) -> {},
                        () -> {
                          Assertions.fail(
                              name
                                  + ": Expect replica "
                                  + fixedReplica
                                  + " not moved, but it appears to disappear from the target allocation");
                        });
              });
    }
  }
}
