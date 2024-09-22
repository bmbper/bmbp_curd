use crate::dao::BmbpCurdDao;
use bmbp_http_type::{BmbpResp, BmbpRespErr};
use bmbp_rdbc::{
    DeleteWrapper, QueryWrapper, RdbcIdent, RdbcOrm, RdbcOrmRow, RdbcTable, RdbcTableFilter,
    RdbcTableWrapper, UpdateWrapper,
};
use serde::Serialize;
use std::fmt::Debug;

pub struct BmbpCurdService;

impl BmbpCurdService {
    pub async fn find_info_by_id<T>(orm: &RdbcOrm, data_id: Option<&String>) -> BmbpResp<Option<T>>
    where
        T: RdbcTable + Default + Debug + Clone + Serialize + From<RdbcOrmRow>,
    {
        if data_id.is_none() {
            return Ok(None);
        }
        let mut query_wrapper = QueryWrapper::new_from::<T>();
        query_wrapper.eq_(T::get_primary_key(), data_id);
        BmbpCurdDao::execute_query_one::<T>(orm, &query_wrapper).await
    }
    pub async fn enable<T>(orm: &RdbcOrm, data_id: Option<&String>) -> BmbpResp<Option<u64>>
    where
        T: RdbcTable + Default + Debug + Clone + Serialize + From<RdbcOrmRow>,
    {
        let dict_info: Option<T> = Self::find_info_by_id::<T>(orm, data_id).await?;
        if dict_info.is_none() {
            return Err(BmbpRespErr::err(
                Some("SERVICE".to_string()),
                Some("未找到字典信息".to_string()),
            ));
        }
        let mut update_wrapper = UpdateWrapper::new();
        update_wrapper.set("data_status", "1");
        update_wrapper.table(T::get_table().get_ident());
        update_wrapper.eq_(T::get_primary_key(), data_id.clone());
        BmbpCurdDao::execute_update::<T>(orm, &update_wrapper).await
    }

    pub async fn batch_enable<T>(orm: &RdbcOrm, data_id: &[String]) -> BmbpResp<Option<u64>>
    where
        T: RdbcTable + Default + Debug + Clone + Serialize + From<RdbcOrmRow>,
    {
        let mut update_wrapper = UpdateWrapper::new();
        update_wrapper.set("data_status", "1");
        update_wrapper.table(T::get_table());
        update_wrapper.in_v_slice(T::get_primary_key(), data_id);
        BmbpCurdDao::execute_update::<T>(orm, &update_wrapper).await
    }

    pub async fn disable<T>(orm: &RdbcOrm, data_id: Option<&String>) -> BmbpResp<Option<u64>>
    where
        T: RdbcTable + Default + Debug + Clone + Serialize + From<RdbcOrmRow>,
    {
        let dict_info = Self::find_info_by_id::<T>(orm, data_id).await?;
        if dict_info.is_none() {
            return Err(BmbpRespErr::err(
                Some("SERVICE".to_string()),
                Some("未找到字典信息".to_string()),
            ));
        }
        let mut update_wrapper = UpdateWrapper::new();
        update_wrapper.set("data_status", "0");
        update_wrapper.table(T::get_table().get_ident());
        update_wrapper.eq_(T::get_primary_key(), data_id.clone());
        BmbpCurdDao::execute_update::<T>(orm, &update_wrapper).await
    }

    pub async fn batch_disable<T>(orm: &RdbcOrm, data_id: &[String]) -> BmbpResp<Option<u64>>
    where
        T: RdbcTable + Default + Debug + Clone + Serialize + From<RdbcOrmRow>,
    {
        let mut update_wrapper = UpdateWrapper::new();
        update_wrapper.set("data_status", "0");
        update_wrapper.table(T::get_table().get_ident());
        update_wrapper.in_v_slice(T::get_primary_key(), data_id);
        BmbpCurdDao::execute_update::<T>(orm, &update_wrapper).await
    }

    pub async fn remove<T>(orm: &RdbcOrm, data_id: Option<&String>) -> BmbpResp<Option<u64>>
    where
        T: RdbcTable + Default + Debug + Clone + Serialize + From<RdbcOrmRow>,
    {
        let dict_info = Self::find_info_by_id::<T>(orm, data_id).await?;
        if dict_info.is_none() {
            return Err(BmbpRespErr::err(
                Some("SERVICE".to_string()),
                Some("未找到字典信息".to_string()),
            ));
        }
        let mut wrapper = DeleteWrapper::new();
        wrapper.table(T::get_table().get_ident());
        wrapper.eq_(T::get_primary_key(), data_id);
        BmbpCurdDao::execute_delete::<T>(orm, &wrapper).await
    }

    pub async fn batch_remove<T>(orm: &RdbcOrm, data_id: &[String]) -> BmbpResp<Option<u64>>
    where
        T: RdbcTable + Default + Debug + Clone + Serialize + From<RdbcOrmRow>,
    {
        let mut wrapper = DeleteWrapper::new();
        wrapper.table(T::get_table().get_ident());
        wrapper.in_v_slice(T::get_primary_key(), data_id);
        BmbpCurdDao::execute_delete::<T>(orm, &wrapper).await
    }

    pub async fn update_order<T>(
        orm: &RdbcOrm,
        data_id: Option<&String>,
        order: Option<&i32>,
    ) -> BmbpResp<Option<u64>>
    where
        T: RdbcTable + Default + Debug + Clone + Serialize + From<RdbcOrmRow>,
    {
        let dict_info = Self::find_info_by_id::<T>(orm, data_id).await?;
        if dict_info.is_none() {
            return Err(BmbpRespErr::err(
                Some("SERVICE".to_string()),
                Some("未找到字典信息".to_string()),
            ));
        }
        let mut update_wrapper = UpdateWrapper::new();
        update_wrapper.set("data_order", order.unwrap_or(&0));
        update_wrapper.table(T::get_table().get_ident());
        update_wrapper.eq_(T::get_primary_key(), data_id.clone());
        BmbpCurdDao::execute_update::<T>(orm, &update_wrapper).await
    }
}
