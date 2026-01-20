# db（数据库）

数据库是 Reth 的核心组件之一，用于对区块头、区块体、交易等数据进行持久化存储。Reth 的数据库由写入磁盘的键值存储组成，并按“表（table）”进行组织。本章一开始可能会显得有点信息量大，但很快你就会熟悉 `db` crate 的结构与使用方式。本章将介绍数据库的结构、各类表，以及 `Database` trait 的工作机制。

<br>

## 表（Tables）

在 Reth 中，数据库通过“表（tables）”来组织。任何实现了 `Table` trait 的结构体，都可以被视为一张表。

[文件：crates/storage/db-api/src/table.rs](https://github.com/paradigmxyz/reth/blob/main/crates/storage/db-api/src/table.rs#L87-L101)

```rust ignore
pub trait Table: Send + Sync + Debug + 'static {
    /// Return table name as it is present inside the MDBX.
    const NAME: &'static str;
    /// Whether the table is also a `DUPSORT` table.
    const DUPSORT: bool;
    /// Key element of `Table`.
    ///
    /// Sorting should be taken into account when encoding this.
    type Key: Key;
    /// Value element of `Table`.
    type Value: Value;
}

//--snip--
pub trait Key: Encode + Decode + Ord + Clone + Serialize + for<'a> Deserialize<'a> {}

//--snip--
pub trait Value: Compress + Decompress + Serialize {}

```

`Table` trait 有两个泛型参数 `Key` 和 `Value`，它们分别需要实现 `Key` 与 `Value` trait。`Encode` trait 负责把数据转换成字节以便存入数据库；`Decode` trait 负责把字节还原成原始数据。同样地，`Compress` 与 `Decompress` trait 用于在写入/读取数据库时对数据进行压缩与解压。

节点中存在大量不同的表，用来存储从 `Headers` 到 `Transactions` 等各种类型的数据。下面是目前所有表的列表。如果你想查看任意表的具体定义，可以参考 [这个链接](https://github.com/paradigmxyz/reth/blob/main/crates/storage/db-api/src/tables/mod.rs)。

- CanonicalHeaders
- HeaderTerminalDifficulties（已弃用）
- HeaderNumbers
- Headers
- BlockBodyIndices
- BlockOmmers
- BlockWithdrawals
- Transactions
- TransactionHashNumbers
- TransactionBlocks
- Receipts
- Bytecodes
- PlainAccountState
- PlainStorageState
- AccountsHistory
- StoragesHistory
- AccountChangeSets
- StorageChangeSets
- HashedAccounts
- HashedStorages
- AccountsTrie
- StoragesTrie
- AccountsTrieChangeSets
- StoragesTrieChangeSets
- TransactionSenders
- StageCheckpoints
- StageCheckpointProgresses
- PruneCheckpoints
- VersionHistory
- ChainState
- Metadata

<br>

## Database

Reth 的数据库设计围绕其核心的 [Database trait](https://github.com/paradigmxyz/reth/blob/main/crates/storage/db-api/src/database.rs#L8-L52) 展开。这个 trait 为多种数据库实现提供统一的功能接口。我们先快速看一下 `Database` trait 的定义以及它如何工作。

[文件：crates/storage/db-api/src/database.rs](https://github.com/paradigmxyz/reth/blob/main/crates/storage/db-api/src/database.rs#L8-L52)

```rust ignore
/// Main Database trait that can open read-only and read-write transactions.
///
/// Sealed trait which cannot be implemented by 3rd parties, exposed only for consumption.
pub trait Database: Send + Sync + Debug {
    /// Read-Only database transaction
    type TX: DbTx + Send + Sync + Debug + 'static;
    /// Read-Write database transaction
    type TXMut: DbTxMut + DbTx + TableImporter + Send + Sync + Debug + 'static;

    /// Create read only transaction.
    #[track_caller]
    fn tx(&self) -> Result<Self::TX, DatabaseError>;

    /// Create read write transaction only possible if database is open with write access.
    #[track_caller]
    fn tx_mut(&self) -> Result<Self::TXMut, DatabaseError>;

    /// Takes a function and passes a read-only transaction into it, making sure it's closed in the
    /// end of the execution.
    fn view<T, F>(&self, f: F) -> Result<T, DatabaseError>
    where
        F: FnOnce(&mut Self::TX) -> T,
    {
        let mut tx = self.tx()?;

        let res = f(&mut tx);
        tx.commit()?;

        Ok(res)
    }

    /// Takes a function and passes a write-read transaction into it, making sure it's committed in
    /// the end of the execution.
    fn update<T, F>(&self, f: F) -> Result<T, DatabaseError>
    where
        F: FnOnce(&Self::TXMut) -> T,
    {
        let tx = self.tx_mut()?;

        let res = f(&tx);
        tx.commit()?;

        Ok(res)
    }
}
```

任何实现了 `Database` trait 的类型，都可以创建数据库事务（transaction），并对事务进行只读访问（view）或读写更新（update）。例如，你可以直接通过 `tx_mut()` 打开一个读写事务，向表写入数据，然后提交：

```rust ignore
let tx = db.tx_mut()?;
tx.put::<tables::CanonicalHeaders>(block_number, block.hash())?;
tx.put::<tables::Headers>(block_number, header.clone())?;
tx.put::<tables::HeaderNumbers>(block.hash(), block_number)?;
tx.commit()?;
```

`Database` 定义了两个关联类型：`TX` 与 `TXMut`。

[文件：crates/storage/db-api/src/database.rs](https://github.com/paradigmxyz/reth/blob/main/crates/storage/db-api/src/database.rs)

`TX` 可以是任何实现了 `DbTx` trait 的类型。`DbTx` 提供了一组用于只读事务的接口：

[文件：crates/storage/db-api/src/transaction.rs](https://github.com/paradigmxyz/reth/blob/main/crates/storage/db-api/src/transaction.rs#L11-L40)

```rust ignore
/// Read only transaction
pub trait DbTx: Debug + Send + Sync {
    /// Cursor type for this read-only transaction
    type Cursor<T: Table>: DbCursorRO<T> + Send + Sync;
    /// `DupCursor` type for this read-only transaction
    type DupCursor<T: DupSort>: DbDupCursorRO<T> + DbCursorRO<T> + Send + Sync;

    /// Get value by an owned key
    fn get<T: Table>(&self, key: T::Key) -> Result<Option<T::Value>, DatabaseError>;
    /// Get value by a reference to the encoded key (avoids cloning for raw keys)
    fn get_by_encoded_key<T: Table>(
        &self,
        key: &<T::Key as Encode>::Encoded,
    ) -> Result<Option<T::Value>, DatabaseError>;
    /// Commit for read only transaction will consume and free transaction and allows
    /// freeing of memory pages
    fn commit(self) -> Result<(), DatabaseError>;
    /// Aborts transaction
    fn abort(self);
    /// Iterate over read only values in table.
    fn cursor_read<T: Table>(&self) -> Result<Self::Cursor<T>, DatabaseError>;
    /// Iterate over read only values in dup sorted table.
    fn cursor_dup_read<T: DupSort>(&self) -> Result<Self::DupCursor<T>, DatabaseError>;
    /// Returns number of entries in the table.
    fn entries<T: Table>(&self) -> Result<usize, DatabaseError>;
    /// Disables long-lived read transaction safety guarantees.
    fn disable_long_read_transaction_safety(&mut self);
}
```

`TXMut` 可以是任何实现了 `DbTxMut` trait 的类型。`DbTxMut` 提供了一组用于读写事务的接口，以及相应的游标（cursor）类型：

[文件：crates/storage/db-api/src/transaction.rs](https://github.com/paradigmxyz/reth/blob/main/crates/storage/db-api/src/transaction.rs)

```rust ignore
/// Read write transaction that allows writing to database
pub trait DbTxMut: Send + Sync {
    /// Read-Write Cursor type
    type CursorMut<T: Table>: DbCursorRW<T> + DbCursorRO<T> + Send + Sync;
    /// Read-Write `DupCursor` type
    type DupCursorMut<T: DupSort>: DbDupCursorRW<T>
        + DbCursorRW<T>
        + DbDupCursorRO<T>
        + DbCursorRO<T>
        + Send
        + Sync;

    /// Put value to database
    fn put<T: Table>(&self, key: T::Key, value: T::Value) -> Result<(), DatabaseError>;
    /// Append value with the largest key to database (fast path)
    fn append<T: Table>(&self, key: T::Key, value: T::Value) -> Result<(), DatabaseError> {
        self.put::<T>(key, value)
    }
    /// Delete value from database
    fn delete<T: Table>(&self, key: T::Key, value: Option<T::Value>)
        -> Result<bool, DatabaseError>;
    /// Clears database.
    fn clear<T: Table>(&self) -> Result<(), DatabaseError>;
    /// Cursor mut
    fn cursor_write<T: Table>(&self) -> Result<Self::CursorMut<T>, DatabaseError>;
    /// `DupCursor` mut.
    fn cursor_dup_write<T: DupSort>(&self) -> Result<Self::DupCursorMut<T>, DatabaseError>;
}
```

下面我们通过几个示例看看 `DbTx` 与 `DbTxMut` 的实际用法。

以 `DatabaseProvider<Tx>` 结构体为例，`DatabaseProvider<Tx>::header_by_number()` 目前会把读取逻辑委托给 static-file provider：

[文件：crates/storage/provider/src/providers/database/mod.rs](https://github.com/paradigmxyz/reth/blob/main/crates/storage/provider/src/providers/database/mod.rs#L280-L282)

```rust ignore
impl<TX: DbTx> HeaderProvider for DatabaseProvider<TX> {
   //--snip--

    fn header_by_number(&self, num: BlockNumber) -> ProviderResult<Option<Self::Header>> {
        self.static_file_provider.header_by_number(num)
    }

   //--snip--
}
```

注意这里使用了 [turbofish](https://techblog.tonsser.com/posts/what-is-rusts-turbofish) 语法来指定在调用 `DbTx::get()` 时所使用的表。看一下 `DbTx::get()` 的定义，它引入了一个实现了 `Table` trait 的泛型参数 `T`：

[文件：crates/storage/db-api/src/transaction.rs](https://github.com/paradigmxyz/reth/blob/main/crates/storage/db-api/src/transaction.rs)

```rust ignore
fn get<T: Table>(&self, key: T::Key) -> Result<Option<T::Value>, DatabaseError>;
```

这种设计模式非常强大，它让 Reth 可以复用 `DbTx` 与 `DbTxMut` 提供的通用方法，而无需为数据库里的每张表单独写一套实现。

我们再看几个例子。在下面的片段中，使用 `DbTxMut::put()` 往 `CanonicalHeaders`、`Headers` 与 `HeaderNumbers` 三张表写入数据：

[文件：crates/storage/provider/src/providers/database/provider.rs](https://github.com/paradigmxyz/reth/blob/main/crates/storage/provider/src/providers/database/provider.rs)

```rust ignore
self.tx.put::<tables::CanonicalHeaders>(block_number, block.hash())?;
self.tx.put::<tables::Headers>(block_number, block.header.clone())?;
self.tx.put::<tables::HeaderNumbers>(block.hash(), block_number)?;
```

再看一下 `DatabaseProviderRW<DB: Database>` 结构体。它用于创建一个可变事务来与数据库交互。`DatabaseProviderRW<DB: Database>` 实现了 `Deref` 与 `DerefMut`，它们会返回对其第一个字段的引用（即 `TxMut`）。回忆一下，`TxMut` 是 `Database` trait 的泛型关联类型：`type TXMut: DbTxMut + DbTx + Send + Sync;`，因此它不仅能写入（`DbTxMut`），也能调用 `DbTx` 的只读方法（例如 `DbTx::get()`）。

下面这个例子展示了如何通过 static-file provider 从静态文件中读取某个区块范围的 headers：

[文件：crates/storage/provider/src/providers/static_file/manager.rs](https://github.com/paradigmxyz/reth/blob/main/crates/storage/provider/src/providers/static_file/manager.rs#L1680-L1690)

```rust ignore
// Read headers for a specific block range from static files
let headers = provider.static_file_provider().headers_range(block_range.clone())?;
```

我们再看一个游标（cursor）的用法示例。下面代码片段来自 `stages` crate 中 `BodyStage` 的 `unwind` 方法。当 pipeline 在执行 body stage 过程中发生错误时，该方法负责把数据库中的变更回滚（unwind）到某个区块高度。

[文件：crates/stages/stages/src/stages/bodies.rs](https://github.com/paradigmxyz/reth/blob/main/crates/stages/stages/src/stages/bodies.rs)

```rust ignore
/// Unwind the stage.
fn unwind(
    &mut self,
    provider: &Provider,
    input: UnwindInput,
) -> Result<UnwindOutput, StageError> {
   self.buffer.take();

   ensure_consistency(provider, Some(input.unwind_to))?;
   provider.remove_bodies_above(input.unwind_to)?;

    Ok(UnwindOutput {
        checkpoint: StageCheckpoint::new(input.unwind_to)
            .with_entities_stage_checkpoint(stage_checkpoint(provider)?),
    })
}
```

这个函数会先为 `BlockBodyIndices`、`BlockOmmers`、`BlockWithdrawals`、`TransactionBlocks` 等表获取可变游标（mutable cursor）。

接着它会获得 block body 游标的 walker，并从当前位置向后（向更小的区块号）遍历游标，把从最新区块一直到 `UnwindInput` 指定高度之间的区块体记录逐个删除。

这只是一个关于游标如何配合数据库表工作的简短示例。后续关于 `libmdbx` crate 的章节会更深入地解释游标如何与数据库交互、底层到底发生了什么。

<br>

## 小结（Summary）

本章信息量较大，我们简单回顾一下：数据库由多张表组成，每张表都是一组键值对，用于表达区块链中各类数据。任何实现了 `Database` trait 的类型，都可以对不同表中的数据进行读取、更新或删除。Reth 的数据库设计通过“嵌套 trait + 关联类型（GAT/associated types）+ 泛型”的组合，为与不同表交互提供了统一且强大的抽象接口。

<br>

# 下一章（Next Chapter）

[下一章](eth-wire_zh.md)

