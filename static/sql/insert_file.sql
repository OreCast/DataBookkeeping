INSERT INTO FILES
    (file_id,logical_file_name,is_file_valid,
     datset_id,meta_id,
     check_sum,file_size,md5,
     creation_date,create_by,
     last_modification_date,last_modified_by)
    VALUES
    (:file_id,:logical_file_name,:is_file_valid,
     :datset_id,:meta_id,
     :check_sum,:file_size,:md5,
     :creation_date,:create_by,
     :last_modification_date,:last_modified_by)
