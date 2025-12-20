# IndexedDF

Apache Spark DataFrameにインデックス機能を追加する拡張ライブラリ。

## 概要

Spark SQLのDataFrameに対してハッシュインデックスを作成し、キールックアップやインデックスを活用したEqui-Joinを高速化する。CTrie（Concurrent Trie）を使用してスレッドセーフなインデックス構造を実現。

## ビルド・テスト・実行

```bash
sbt compile    # コンパイル
sbt test       # テスト実行

# デモ実行（インメモリデータ）
sbt "runMain indexeddataframe.Example"

# ベンチマーク実行（インメモリデータ）
sbt "runMain indexeddataframe.BenchmarkPrograms"

# ベンチマーク実行（外部CSVファイル）
sbt "runMain indexeddataframe.BenchmarkPrograms <delimiter1> <path1> <delimiter2> <path2> <partitions> <master>"
```

## 依存関係

- Scala 2.13.16
- Apache Spark 4.0.0
- ScalaTest 3.2.19

## アーキテクチャ

### 主要コンポーネント

```
src/main/
├── java/indexeddataframe/
│   └── RowBatch.java             # オフヒープメモリ管理（行データ格納）
└── scala/
    ├── indexeddataframe/
    │   ├── InternalIndexedPartition.scala   # インデックス付きパーティションのコアデータ構造
    │   ├── Utils.scala               # ユーティリティとIRDD（カスタムRDD）
    │   ├── implicits.scala           # Dataset拡張のimplicit変換
    │   ├── strategies.scala          # Catalyst物理プランへの変換戦略
    │   ├── execution/
    │   │   └── operators.scala       # 物理演算子（SparkPlan実装）
    │   └── logical/
    │       ├── operators.scala       # 論理演算子（LogicalPlan実装）
    │       └── rules.scala           # Catalyst最適化ルール
    └── org/apache/spark/sql/
        ├── IndexedDatasetFunctions.scala  # DatasetへのAPI拡張
        └── InMemoryRelationMatcher.scala  # キャッシュ検出用パターンマッチャー
```

### データ構造

- **RowBatch** (`RowBatch.java`): オフヒープメモリ上の行データストレージ
  - `Platform.allocateMemory()`でJVMヒープ外に4MBバッチを確保
  - GCの影響を受けず、大量データでも安定したパフォーマンス
  - `Platform.copyMemory()`による高速なメモリ操作

- **InternalIndexedPartition**: パーティション単位のインデックス付きデータ構造
  - `TrieMap[Long, Long]`: キー → 行ポインタのインデックス（CTrie）
  - `TrieMap[Int, RowBatch]`: 行データを格納するバッチ
  - 64bit整数にバッチNo、オフセット、サイズをパック

- **IRDD**: `RDD[InternalIndexedPartition]`のラッパー。`get`/`multiget`メソッドを提供

### Catalyst統合

1. **論理演算子** (`logical/operators.scala`)
   - `CreateIndex`, `AppendRows`, `GetRows`, `IndexedFilter`, `IndexedJoin`

2. **物理演算子** (`execution/operators.scala`)
   - `CreateIndexExec`, `AppendRowsExec`, `GetRowsExec`, `IndexedFilterExec`
   - `IndexedShuffledEquiJoinExec`, `IndexedBroadcastEquiJoinExec`

3. **最適化ルール** (`logical/rules.scala`)
   - `ConvertToIndexedOperators`: Join/Filterをインデックス演算子に変換

4. **戦略** (`strategies.scala`)
   - `IndexedOperators`: 論理プラン → 物理プランへの変換

## 使用方法

```scala
import indexeddataframe.implicits._
import indexeddataframe.IndexedOperators
import indexeddataframe.logical.ConvertToIndexedOperators

// 戦略と最適化ルールを登録
sparkSession.experimental.extraStrategies ++= Seq(IndexedOperators)
sparkSession.experimental.extraOptimizations ++= Seq(ConvertToIndexedOperators)

// インデックス作成（カラム0にインデックス）
val indexedDF = df.createIndex(0).cache()

// キールックアップ
val rows = indexedDF.getRows(1234)

// 行追加（Copy-on-Write）
val newDF = indexedDF.appendRows(moreRows)

// SQLでのJoin（インデックス側がleftの場合、自動的にIndexedJoinに変換）
indexedDF.createOrReplaceTempView("indexed_table")
otherDF.createOrReplaceTempView("other_table")
spark.sql("SELECT * FROM indexed_table JOIN other_table ON indexed_table.key = other_table.key")
```

## 注意事項

- **AQE非互換**: Adaptive Query Execution (AQE)との互換性問題があるため、`spark.sql.adaptive.enabled=false`を設定する必要がある
- **キャッシュ必須**: `createIndex()`後に`.cache()`を呼ぶことでインデックスが永続化される
- **サポートするキー型**: Long, Int, String（ハッシュ化）, Double

## テスト

```bash
sbt test
```

テストケース: createIndex, getRows, filter, appendRows, join, join2, string index, divergence

## 制限事項

- **サポートするJoin Type**: 現在はINNER JOINのみをサポート。Semi-Join、Anti-Join、Outer-Joinが検出された場合は通常のSpark Joinにフォールバックする

## Future Work

- **Semi-Join対応**: LEFT SEMI JOIN、LEFT ANTI JOINをインデックスを活用して高速化
- **Outer-Join対応**: LEFT/RIGHT/FULL OUTER JOINをインデックスを活用して高速化
- **AQE対応**: Adaptive Query Execution (AQE) との互換性改善
- **複合キーインデックス**: 複数カラムを組み合わせたインデックスのサポート
- **最適化ルールの拡充**: より多くのクエリパターンに対するインデックス利用の最適化ルール追加
