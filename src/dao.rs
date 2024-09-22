use bmbp_http_type::{BmbpResp, BmbpRespErr, PageData};
use bmbp_rdbc::{DeleteWrapper, InsertWrapper, QueryWrapper, RdbcOrm, UpdateWrapper};
use bmbp_rdbc::{RdbcOrmRow, RdbcTable};
use serde::Serialize;
use std::fmt::Debug;

pub struct BmbpCurdDao;

impl BmbpCurdDao {
    pub async fn execute_query_page<T>(
        orm: &RdbcOrm,
        pag_num: Option<usize>,
        page_size: Option<usize>,
        query_wrapper: &QueryWrapper,
    ) -> BmbpResp<Option<PageData<T>>>
    where
        T: RdbcTable + Default + Debug + Clone + Serialize + From<RdbcOrmRow> + Send + Sync,
    {
        return match orm
            .select_page_by_query::<T>(
                pag_num.unwrap_or(1),
                page_size.unwrap_or(10),
                &query_wrapper,
            )
            .await
        {
            Ok(mut orm_page) => {
                let orm_page_data = orm_page.data_take();
                let resp_page = PageData::new(
                    orm_page.page_num().clone() as u32,
                    orm_page.page_size().clone() as u32,
                    orm_page.total().clone() as u32,
                    orm_page_data.unwrap_or(vec![]),
                );
                Ok(Some(resp_page))
            }
            Err(err) => Err(BmbpRespErr::err(
                Some("DB".to_string()),
                Some(err.get_msg()),
            )),
        };
    }
    pub async fn execute_query_list<T>(
        orm: &RdbcOrm,
        query_wrapper: &QueryWrapper,
    ) -> BmbpResp<Option<Vec<T>>>
    where
        T: RdbcTable + Default + Debug + Clone + Serialize + From<RdbcOrmRow>,
    {
        match orm.select_list_by_query::<T>(&query_wrapper).await {
            Ok(dict) => Ok(dict),
            Err(err) => Err(BmbpRespErr::err(
                Some("DB".to_string()),
                Some(err.get_msg()),
            )),
        }
    }
    pub async fn execute_query_one<T>(
        orm: &RdbcOrm,
        query_wrapper: &QueryWrapper,
    ) -> BmbpResp<Option<T>>
    where
        T: RdbcTable + Default + Debug + Clone + Serialize + From<RdbcOrmRow>,
    {
        match orm.select_one_by_query::<T>(&query_wrapper).await {
            Ok(dict) => Ok(dict),
            Err(err) => Err(BmbpRespErr::err(
                Some("DB".to_string()),
                Some(err.get_msg()),
            )),
        }
    }
    pub async fn execute_insert<T>(
        orm: &RdbcOrm,
        insert_wrapper: &InsertWrapper,
    ) -> BmbpResp<Option<u64>>
    where
        T: RdbcTable + From<RdbcOrmRow>,
    {
        match orm.execute_insert(insert_wrapper).await {
            Ok(dict) => Ok(Some(dict)),
            Err(err) => Err(BmbpRespErr::err(
                Some("DB".to_string()),
                Some(err.get_msg()),
            )),
        }
    }
    pub async fn execute_update<T>(
        orm: &RdbcOrm,
        update_wrapper: &UpdateWrapper,
    ) -> BmbpResp<Option<u64>>
    where
        T: RdbcTable + From<RdbcOrmRow>,
    {
        match orm.execute_update(&update_wrapper).await {
            Ok(row_count) => Ok(Some(row_count)),
            Err(err) => Err(BmbpRespErr::err(
                Some("DB".to_string()),
                Some(err.get_msg()),
            )),
        }
    }
    pub async fn execute_delete<T>(
        orm: &RdbcOrm,
        delete_wrapper: &DeleteWrapper,
    ) -> BmbpResp<Option<u64>>
    where
        T: RdbcTable + From<RdbcOrmRow>,
    {
        match orm.execute_delete(&delete_wrapper).await {
            Ok(row_count) => Ok(Some(row_count)),
            Err(err) => Err(BmbpRespErr::err(
                Some("DB".to_string()),
                Some(err.get_msg()),
            )),
        }
    }
}
