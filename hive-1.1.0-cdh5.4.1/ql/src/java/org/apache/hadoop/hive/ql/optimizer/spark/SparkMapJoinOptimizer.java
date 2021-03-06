/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.optimizer.spark;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.MuxOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.optimizer.BucketMapjoinProc;
import org.apache.hadoop.hive.ql.optimizer.MapJoinProcessor;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.spark.OptimizeSparkProcContext;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.OpTraits;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;

/**
 * SparkMapJoinOptimizer cloned from ConvertJoinMapJoin is an optimization that replaces a common join
 * (aka shuffle join) with a map join (aka broadcast or fragment replicate
 * join when possible. Map joins have restrictions on which joins can be
 * converted (e.g.: full outer joins cannot be handled as map joins) as well
 * as memory restrictions (one side of the join has to fit into memory).
 */
public class SparkMapJoinOptimizer implements NodeProcessor {

  private static final Log LOG = LogFactory.getLog(SparkMapJoinOptimizer.class.getName());

  @Override
  /**
   * We should ideally not modify the tree we traverse. However,
   * since we need to walk the tree at any time when we modify the operator, we
   * might as well do it here.
   */
  public Object
      process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs)
          throws SemanticException {

    OptimizeSparkProcContext context = (OptimizeSparkProcContext) procCtx;
    HiveConf conf = context.getConf();
    JoinOperator joinOp = (JoinOperator) nd;

    if (!conf.getBoolVar(HiveConf.ConfVars.HIVECONVERTJOIN)) {
      return null;
    }

    LOG.info("Check if it can be converted to map join");
    long[] mapJoinInfo = getMapJoinConversionInfo(joinOp, context);
    int mapJoinConversionPos = (int) mapJoinInfo[0];

    if (mapJoinConversionPos < 0) {
      return null;
    }

    int numBuckets = -1;
    List<List<String>> bucketColNames = null;

    LOG.info("Convert to non-bucketed map join");
    MapJoinOperator mapJoinOp = convertJoinMapJoin(joinOp, context, mapJoinConversionPos);
    if (conf.getBoolVar(HiveConf.ConfVars.HIVEOPTBUCKETMAPJOIN)) {
      LOG.info("Check if it can be converted to bucketed map join");
      numBuckets = convertJoinBucketMapJoin(joinOp, mapJoinOp,
        context, mapJoinConversionPos);
      if (numBuckets > 1) {
        LOG.info("Converted to map join with " + numBuckets + " buckets");
        bucketColNames = joinOp.getOpTraits().getBucketColNames();
        mapJoinInfo[2] /= numBuckets;
      } else {
        LOG.info("Can not convert to bucketed map join");
      }
    }

    // we can set the traits for this join operator
    OpTraits opTraits = new OpTraits(bucketColNames, numBuckets, null);
    mapJoinOp.setOpTraits(opTraits);
    mapJoinOp.setStatistics(joinOp.getStatistics());
    setNumberOfBucketsOnChildren(mapJoinOp);

    context.getMjOpSizes().put(mapJoinOp, mapJoinInfo[1] + mapJoinInfo[2]);

    return mapJoinOp;
  }

  private void setNumberOfBucketsOnChildren(Operator<? extends OperatorDesc> currentOp) {
    int numBuckets = currentOp.getOpTraits().getNumBuckets();
    for (Operator<? extends OperatorDesc> op : currentOp.getChildOperators()) {
      if (!(op instanceof ReduceSinkOperator) && !(op instanceof GroupByOperator)) {
        op.getOpTraits().setNumBuckets(numBuckets);
        if (numBuckets < 0) {
          op.getOpTraits().setBucketColNames(null);
        }
        setNumberOfBucketsOnChildren(op);
      }
    }
  }

  private int convertJoinBucketMapJoin(JoinOperator joinOp, MapJoinOperator mapJoinOp,
      OptimizeSparkProcContext context, int bigTablePosition) throws SemanticException {
    ParseContext parseContext = context.getParseContext();
    List<String> joinAliases = new ArrayList<String>();
    String baseBigAlias = null;
    Map<Integer, Set<String>> posToAliasMap = joinOp.getPosToAliasMap();
    for (Map.Entry<Integer, Set<String>> entry: posToAliasMap.entrySet()) {
      if (entry.getKey().intValue() == bigTablePosition) {
        baseBigAlias = entry.getValue().iterator().next();
      }
      for (String alias: entry.getValue()) {
        if (!joinAliases.contains(alias)) {
          joinAliases.add(alias);
        }
      }
    }
    mapJoinOp.setPosToAliasMap(posToAliasMap);
    BucketMapjoinProc.checkAndConvertBucketMapJoin(
      parseContext,
      mapJoinOp,
      baseBigAlias,
      joinAliases);
    MapJoinDesc joinDesc = mapJoinOp.getConf();
    return joinDesc.isBucketMapJoin()
      ? joinDesc.getBigTableBucketNumMapping().size() : -1;
  }

  /**
   *   This method returns the big table position in a map-join. If the given join
   *   cannot be converted to a map-join (This could happen for several reasons - one
   *   of them being presence of 2 or more big tables that cannot fit in-memory), it returns -1.
   *
   *   Otherwise, it returns an int value that is the index of the big table in the set
   *   MapJoinProcessor.bigTableCandidateSet
   *
   * @param joinOp
   * @param context
   * @return an array of 3 long values, first value is the position,
   *   second value is the connected map join size, and the third is big table data size.
   */
  private long[] getMapJoinConversionInfo(
      JoinOperator joinOp, OptimizeSparkProcContext context) {
    Set<Integer> bigTableCandidateSet =
        MapJoinProcessor.getBigTableCandidates(joinOp.getConf().getConds());

    long maxSize = context.getConf().getLongVar(
        HiveConf.ConfVars.HIVECONVERTJOINNOCONDITIONALTASKTHRESHOLD);

    int bigTablePosition = -1;

    Statistics bigInputStat = null;
    long totalSize = 0;
    int pos = 0;

    // bigTableFound means we've encountered a table that's bigger than the
    // max. This table is either the the big table or we cannot convert.
    boolean bigTableFound = false;

    for (Operator<? extends OperatorDesc> parentOp : joinOp.getParentOperators()) {

      Statistics currInputStat = parentOp.getStatistics();
      if (currInputStat == null) {
        LOG.warn("Couldn't get statistics from: " + parentOp);
        return new long[]{-1, 0, 0};
      }

      // Union is hard to handle. For instance, the following case:
      //  TS    TS
      //  |      |
      //  FIL   FIL
      //  |      |
      //  SEL   SEL
      //    \   /
      //    UNION
      //      |
      //      RS
      //      |
      //     JOIN
      // If we treat this as a MJ case, then after the RS is removed, we would
      // create two MapWorks, for each of the TS. Each of these MapWork will contain
      // a MJ operator, which is wrong.
      // Otherwise, we could try to break the op tree at the UNION, and create two MapWorks
      // for the branches above. Then, MJ will be in the following ReduceWork.
      // But, this is tricky to implement, and we'll leave it as a future work for now.
      if (containUnionWithoutRS(parentOp.getParentOperators().get(0))) {
        return new long[]{-1, 0, 0};
      }

      long inputSize = currInputStat.getDataSize();
      if ((bigInputStat == null)
          || ((bigInputStat != null)
          && (inputSize > bigInputStat.getDataSize()))) {

        if (bigTableFound) {
          // cannot convert to map join; we've already chosen a big table
          // on size and there's another one that's bigger.
          return new long[]{-1, 0, 0};
        }

        if (inputSize > maxSize) {
          if (!bigTableCandidateSet.contains(pos)) {
            // can't use the current table as the big table, but it's too
            // big for the map side.
            return new long[]{-1, 0, 0};
          }

          bigTableFound = true;
        }

        if (bigInputStat != null) {
          // we're replacing the current big table with a new one. Need
          // to count the current one as a map table then.
          totalSize += bigInputStat.getDataSize();
        }

        if (totalSize > maxSize) {
          // sum of small tables size in this join exceeds configured limit
          // hence cannot convert.
          return new long[]{-1, 0, 0};
        }

        if (bigTableCandidateSet.contains(pos)) {
          bigTablePosition = pos;
          bigInputStat = currInputStat;
        }
      } else {
        totalSize += currInputStat.getDataSize();
        if (totalSize > maxSize) {
          // cannot hold all map tables in memory. Cannot convert.
          return new long[]{-1, 0, 0};
        }
      }
      pos++;
    }

    if (bigTablePosition == -1) {
      //No big table candidates.
      return new long[]{-1, 0, 0};
    }

    //Final check, find size of already-calculated Mapjoin Operators in same work (spark-stage).
    //We need to factor this in to prevent overwhelming Spark executor-memory.
    long connectedMapJoinSize = getConnectedMapJoinSize(joinOp.getParentOperators().
      get(bigTablePosition), joinOp, context);
    if ((connectedMapJoinSize + totalSize) > maxSize) {
      return new long[]{-1, 0, 0};
    }

    return new long[]{bigTablePosition, connectedMapJoinSize, totalSize};
  }

  /**
   * Examines this operator and all the connected operators, for mapjoins that will be in the same work.
   * @param parentOp potential big-table parent operator, explore up from this.
   * @param joinOp potential mapjoin operator, explore down from this.
   * @param ctx context to pass information.
   * @return total size of parent mapjoins in same work as this operator.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private long getConnectedMapJoinSize(Operator<? extends OperatorDesc> parentOp, Operator joinOp,
    OptimizeSparkProcContext ctx) {
    long result = 0;
    for (Operator<? extends OperatorDesc> grandParentOp : parentOp.getParentOperators()) {
      result += getConnectedParentMapJoinSize(grandParentOp, ctx);
    }
    result += getConnectedChildMapJoinSize(joinOp, ctx);
    return result;
  }

  /**
   * Examines this operator and all the parents, for mapjoins that will be in the same work.
   * @param op given operator
   * @param ctx context to pass information.
   * @return total size of parent mapjoins in same work as this operator.
   */
  private long getConnectedParentMapJoinSize(Operator<? extends OperatorDesc> op, OptimizeSparkProcContext ctx) {
    if ((op instanceof UnionOperator) || (op instanceof ReduceSinkOperator)) {
      //Work Boundary, stop exploring.
      return 0;
    }

    if (op instanceof MapJoinOperator) {
      //found parent mapjoin operator.  Its size should already reflect any other mapjoins connected to it.
      long mjSize = ctx.getMjOpSizes().get(op);
      return mjSize;
    }

    long result = 0;
    for (Operator<? extends OperatorDesc> parentOp : op.getParentOperators()) {
      //Else, recurse up the parents.
      result += getConnectedParentMapJoinSize(parentOp, ctx);
    }
    return result;
  }

  /**
   * Examines this operator and all the children, for mapjoins that will be in the same work.
   * @param op given operator
   * @param ctx context to pass information.
   * @return total size of child mapjoins in same work as this operator.
   */
  private long getConnectedChildMapJoinSize(Operator<? extends OperatorDesc> op, OptimizeSparkProcContext ctx) {
    if ((op instanceof UnionOperator) || (op instanceof ReduceSinkOperator)) {
      //Work Boundary, stop exploring.
      return 0;
    }

    if (op instanceof MapJoinOperator) {
      //Found child mapjoin operator.
      //Its size should already reflect any mapjoins connected to it, so stop processing.
      long mjSize = ctx.getMjOpSizes().get(op);
      return mjSize;
    }

    long result = 0;
    for (Operator<? extends OperatorDesc> childOp : op.getChildOperators()) {
      //Else, recurse to the children.
      result += getConnectedChildMapJoinSize(childOp, ctx);
    }
    return result;
  }

  /*
   * Once we have decided on the map join, the tree would transform from
   *
   *        |                   |
   *       Join               MapJoin
   *       / \                /   \
   *     RS   RS   --->     RS    TS (big table)
   *    /      \           /
   *   TS       TS        TS (small table)
   *
   * for spark.
   */

  public MapJoinOperator convertJoinMapJoin(JoinOperator joinOp, OptimizeSparkProcContext context,
      int bigTablePosition) throws SemanticException {
    // bail on mux operator because currently the mux operator masks the emit keys
    // of the constituent reduce sinks.
    for (Operator<? extends OperatorDesc> parentOp : joinOp.getParentOperators()) {
      if (parentOp instanceof MuxOperator) {
        return null;
      }
    }

    //can safely convert the join to a map join.
    ParseContext parseContext = context.getParseContext();
    MapJoinOperator mapJoinOp =
        MapJoinProcessor.convertJoinOpMapJoinOp(context.getConf(), parseContext.getOpParseCtx(), joinOp,
            joinOp.getConf().isLeftInputJoin(), joinOp.getConf().getBaseSrc(), joinOp.getConf().getMapAliases(),
            bigTablePosition, true);

    Operator<? extends OperatorDesc> parentBigTableOp =
        mapJoinOp.getParentOperators().get(bigTablePosition);
    if (parentBigTableOp instanceof ReduceSinkOperator) {
      mapJoinOp.getParentOperators().remove(bigTablePosition);
      if (!(mapJoinOp.getParentOperators().contains(parentBigTableOp.getParentOperators().get(0)))) {
        mapJoinOp.getParentOperators().add(bigTablePosition,
            parentBigTableOp.getParentOperators().get(0));
      }
      parentBigTableOp.getParentOperators().get(0).removeChild(parentBigTableOp);
      for (Operator<? extends OperatorDesc> op : mapJoinOp.getParentOperators()) {
        if (!(op.getChildOperators().contains(mapJoinOp))) {
          op.getChildOperators().add(mapJoinOp);
        }
        op.getChildOperators().remove(joinOp);
      }
    }

    // Data structures
    mapJoinOp.getConf().setQBJoinTreeProps(joinOp.getConf());

    return mapJoinOp;
  }

  private boolean containUnionWithoutRS(Operator<? extends OperatorDesc> op) {
    boolean result = false;
    if (op instanceof UnionOperator) {
      for (Operator<? extends OperatorDesc> pop : op.getParentOperators()) {
        if (!(pop instanceof ReduceSinkOperator)) {
          result = true;
          break;
        }
      }
    } else if (op instanceof ReduceSinkOperator) {
      result = false;
    } else {
      for (Operator<? extends OperatorDesc> pop : op.getParentOperators()) {
        if (containUnionWithoutRS(pop)) {
          result = true;
          break;
        }
      }
    }
    return result;
  }
}
