INSERT INTO DATASETS
    (user_id,login,first_name,last_name,password,
     creation_date,create_by,
     last_modification_date,last_modified_by)
    VALUES
    (:user_id,:login,:first_name,:last_name,:password,
     :creation_date,:create_by,
     :last_modification_date,:last_modified_by)
