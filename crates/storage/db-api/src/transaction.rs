use crate::{
    cursor::{DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW},
    table::{DupSort, Encode, Table},
    DatabaseError,
};
use std::fmt::Debug;

/// 用于访问 [`DbTx`] 游标的辅助适配器类型。
pub type CursorTy<TX, T> = <TX as DbTx>::Cursor<T>;

/// 用于访问 [`DbTx`] 重复键游标（dup cursor）的辅助适配器类型。
pub type DupCursorTy<TX, T> = <TX as DbTx>::DupCursor<T>;

/// 用于访问 [`DbTxMut`] 可变游标的辅助适配器类型。
pub type CursorMutTy<TX, T> = <TX as DbTxMut>::CursorMut<T>;

/// 用于访问 [`DbTxMut`] 可变重复键游标的辅助适配器类型。
pub type DupCursorMutTy<TX, T> = <TX as DbTxMut>::DupCursorMut<T>;

/// 只读事务 trait
pub trait DbTx: Debug + Send {
    /// 此只读事务的游标类型
    type Cursor<T: Table>: DbCursorRO<T> + Send + Sync;
    /// 此只读事务的重复键游标（`DupCursor`）类型
    type DupCursor<T: DupSort>: DbDupCursorRO<T> + DbCursorRO<T> + Send + Sync;

    /// 通过 key 获取值
    fn get<T: Table>(&self, key: T::Key) -> Result<Option<T::Value>, DatabaseError>;

    /// 通过已编码 key 的引用获取值，这对于 Address 和 B256 等直接编码为自身的“原始”键特别有用。
    /// 与 `get` 不同，它不需要克隆引用键。
    fn get_by_encoded_key<T: Table>(
        &self,
        key: &<T::Key as Encode>::Encoded,
    ) -> Result<Option<T::Value>, DatabaseError>;

    /// 只读事务的提交将消耗并释放事务，从而允许释放内存页。
    fn commit(self) -> Result<(), DatabaseError>;

    /// 中止事务
    fn abort(self);

    /// 在表中迭代只读值。
    fn cursor_read<T: Table>(&self) -> Result<Self::Cursor<T>, DatabaseError>;

    /// 在支持重复排序的表中迭代只读值。
    fn cursor_dup_read<T: DupSort>(&self) -> Result<Self::DupCursor<T>, DatabaseError>;

    /// 返回表中的条目数量。
    fn entries<T: Table>(&self) -> Result<usize, DatabaseError>;

    /// 禁用长期只读事务的安全保证。
    fn disable_long_read_transaction_safety(&mut self);
}

/// 允许写入数据库的读写事务 trait
pub trait DbTxMut: Send {
    /// 读写游标类型
    type CursorMut<T: Table>: DbCursorRW<T> + DbCursorRO<T> + Send + Sync;
    /// 读写重复键游标（`DupCursor`）类型
    type DupCursorMut<T: DupSort>: DbDupCursorRW<T>
        + DbCursorRW<T>
        + DbDupCursorRO<T>
        + DbCursorRO<T>
        + Send
        + Sync;

    /// 将值放入数据库
    fn put<T: Table>(&self, key: T::Key, value: T::Value) -> Result<(), DatabaseError>;

    /// 将具有最大键的值追加到数据库。这应该与 `put` 有相同的结果，
    /// 但像 MDBX 这样的数据库提供了专用模式使其更快，通常由于无需查找，性能从 O(logN) 降至 O(1)。
    fn append<T: Table>(&self, key: T::Key, value: T::Value) -> Result<(), DatabaseError> {
        self.put::<T>(key, value)
    }

    /// 从数据库中删除值
    fn delete<T: Table>(&self, key: T::Key, value: Option<T::Value>)
        -> Result<bool, DatabaseError>;

    /// 清空数据库。
    fn clear<T: Table>(&self) -> Result<(), DatabaseError>;

    /// 创建可变游标。
    fn cursor_write<T: Table>(&self) -> Result<Self::CursorMut<T>, DatabaseError>;

    /// 创建可变重复键游标。
    fn cursor_dup_write<T: DupSort>(&self) -> Result<Self::DupCursorMut<T>, DatabaseError>;
}

