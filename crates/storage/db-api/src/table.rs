use crate::{
    cursor::{DbCursorRO, DbCursorRW, DbDupCursorRO, DbDupCursorRW},
    transaction::{DbTx, DbTxMut},
    DatabaseError,
};

use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// 能够将数据转换为（理想情况下）压缩格式以保存到数据库的 trait
pub trait Compress: Send + Sync + Sized + Debug {
    /// 压缩后的类型。
    type Compressed: bytes::BufMut
        + AsRef<[u8]>
        + AsMut<[u8]>
        + Into<Vec<u8>>
        + Default
        + Send
        + Sync
        + Debug;

    /// 如果类型无法压缩，则将其内部引用返回为 `Some(self.as_ref())`
    fn uncompressable_ref(&self) -> Option<&[u8]> {
        None
    }

    /// 压缩进入数据库的数据。
    fn compress(self) -> Self::Compressed {
        let mut buf = Self::Compressed::default();
        self.compress_to_buf(&mut buf);
        buf
    }

    /// 将数据压缩到给定的缓冲区中。
    fn compress_to_buf<B: bytes::BufMut + AsMut<[u8]>>(&self, buf: &mut B);
}

/// 能够转换从数据库读取的数据的 trait。
pub trait Decompress: Send + Sync + Sized + Debug {
    /// 解压来自数据库的数据。
    fn decompress(value: &[u8]) -> Result<Self, DatabaseError>;

    /// 解压来自数据库的所有权数据。
    fn decompress_owned(value: Vec<u8>) -> Result<Self, DatabaseError> {
        Self::decompress(&value)
    }
}

/// 能够转换要保存到数据库的数据的 trait。
pub trait Encode: Send + Sync + Sized + Debug {
    /// 编码后的类型。
    type Encoded: AsRef<[u8]> + Into<Vec<u8>> + Send + Sync + Ord + Debug;

    /// 编码进入数据库的数据。
    fn encode(self) -> Self::Encoded;
}

/// 能够转换从数据库读取的数据的 trait。
pub trait Decode: Send + Sync + Sized + Debug {
    /// 解码来自数据库的数据。
    fn decode(value: &[u8]) -> Result<Self, DatabaseError>;

    /// 解码来自数据库的所有权数据。
    fn decode_owned(value: Vec<u8>) -> Result<Self, DatabaseError> {
        Self::decode(&value)
    }
}

/// 强制数据库键实现 [`Encode`] 和 [`Decode`] 的泛型 trait。
pub trait Key: Encode + Decode + Ord + Clone + Serialize + for<'a> Deserialize<'a> {}

impl<T> Key for T where T: Encode + Decode + Ord + Clone + Serialize + for<'a> Deserialize<'a> {}

/// 强制数据库值实现 [`Compress`] 和 [`Decompress`] 的泛型 trait。
pub trait Value: Compress + Decompress + Serialize {}

impl<T> Value for T where T: Compress + Decompress + Serialize {}

/// 数据库表应该遵循的通用 trait。
///
/// [`Table::Key`] 和 [`Table::Value`] 类型应在适当的时候实现 [`Encode`] 和
/// [`Decode`]。这些 trait 定义了数据如何在数据库中存储和读取。
pub trait Table: Send + Sync + Debug + 'static {
    /// 表名。
    const NAME: &'static str;

    /// 该表是否也是 `DUPSORT` 表（允许重复键）。
    const DUPSORT: bool;

    /// `Table` 的键元素。
    ///
    /// 编码时应考虑排序。
    type Key: Key;

    /// `Table` 的值元素。
    type Value: Value;
}

/// 提供对表元数据的对象安全（object-safe）访问的 trait。
pub trait TableInfo: Send + Sync + Debug + 'static {
    /// 表名。
    fn name(&self) -> &'static str;

    /// 该表是否为 `DUPSORT` 表。
    fn is_dupsort(&self) -> bool;
}

/// 包含 `T::Key` 和 `T::Value` 的元组。
pub type TableRow<T> = (<T as Table>::Key, <T as Table>::Value);

/// `DupSort` 允许键在数据库中重复。
pub trait DupSort: Table {
    /// 表子键。此类型必须实现 [`Encode`] 和 [`Decode`]。
    ///
    /// 编码时应考虑排序。
    type SubKey: Key;
}

/// 允许跨数据库复制表
pub trait TableImporter: DbTxMut {
    /// 从另一个事务导入所有表数据。
    fn import_table<T: Table, R: DbTx>(&self, source_tx: &R) -> Result<(), DatabaseError> {
        let mut destination_cursor = self.cursor_write::<T>()?;

        for kv in source_tx.cursor_read::<T>()?.walk(None)? {
            let (k, v) = kv?;
            destination_cursor.append(k, &v)?;
        }

        Ok(())
    }

    /// 在指定范围内从另一个事务导入表数据。
    ///
    /// 此方法对常规表和 `DupSort` 表均能正确工作。对于 `DupSort` 表，
    /// 导入期间会保留范围内所有重复的条目。
    fn import_table_with_range<T: Table, R: DbTx>(
        &self,
        source_tx: &R,
        from: Option<<T as Table>::Key>,
        to: <T as Table>::Key,
    ) -> Result<(), DatabaseError>
    where
        T::Key: Default,
    {
        let mut destination_cursor = self.cursor_write::<T>()?;
        let mut source_cursor = source_tx.cursor_read::<T>()?;

        let source_range = match from {
            Some(from) => source_cursor.walk_range(from..=to),
            None => source_cursor.walk_range(..=to),
        };
        for row in source_range? {
            let (key, value) = row?;
            destination_cursor.append(key, &value)?;
        }

        Ok(())
    }

    /// 从另一个事务导入所有 dupsort 数据。
    fn import_dupsort<T: DupSort, R: DbTx>(&self, source_tx: &R) -> Result<(), DatabaseError> {
        let mut destination_cursor = self.cursor_dup_write::<T>()?;
        let mut cursor = source_tx.cursor_dup_read::<T>()?;

        while let Some((k, _)) = cursor.next_no_dup()? {
            for kv in cursor.walk_dup(Some(k), None)? {
                let (k, v) = kv?;
                destination_cursor.append_dup(k, v)?;
            }
        }

        Ok(())
    }
}
