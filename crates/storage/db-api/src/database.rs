use crate::{
    table::TableImporter,
    transaction::{DbTx, DbTxMut},
    DatabaseError,
};
use std::{fmt::Debug, sync::Arc};

/// 数据库核心 trait，能够开启只读和读写事务。
///
/// 这是一个密封 trait（Sealed trait），不能被外部第三方实现，仅供使用。
pub trait Database: Send + Sync + Debug {
    /// 只读数据库事务类型
    type TX: DbTx + Send + Sync + Debug + 'static;
    /// 读写数据库事务类型
    type TXMut: DbTxMut + DbTx + TableImporter + Send + Sync + Debug + 'static;

    /// 创建只读事务。
    #[track_caller]
    fn tx(&self) -> Result<Self::TX, DatabaseError>;

    /// 创建读写事务。只有当数据库以写权限打开时才可行。
    #[track_caller]
    fn tx_mut(&self) -> Result<Self::TXMut, DatabaseError>;

    /// 接收一个函数并传入只读事务，确保执行结束后事务被关闭。
    fn view<T, F>(&self, f: F) -> Result<T, DatabaseError>
    where
        F: FnOnce(&mut Self::TX) -> T,
    {
        let mut tx = self.tx()?;

        let res = f(&mut tx);
        tx.commit()?;

        Ok(res)
    }

    /// 接收一个函数并传入读写事务，确保执行结束后事务被提交。
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

impl<DB: Database> Database for Arc<DB> {
    type TX = <DB as Database>::TX;
    type TXMut = <DB as Database>::TXMut;

    fn tx(&self) -> Result<Self::TX, DatabaseError> {
        <DB as Database>::tx(self)
    }

    fn tx_mut(&self) -> Result<Self::TXMut, DatabaseError> {
        <DB as Database>::tx_mut(self)
    }
}

impl<DB: Database> Database for &DB {
    type TX = <DB as Database>::TX;
    type TXMut = <DB as Database>::TXMut;

    fn tx(&self) -> Result<Self::TX, DatabaseError> {
        <DB as Database>::tx(self)
    }

    fn tx_mut(&self) -> Result<Self::TXMut, DatabaseError> {
        <DB as Database>::tx_mut(self)
    }
}

