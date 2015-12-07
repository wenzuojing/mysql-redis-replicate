
CREATE USER canal IDENTIFIED BY 'canal';
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'canal'@'%';
-- GRANT ALL PRIVILEGES ON *.* TO 'canal'@'%' ;
FLUSH PRIVILEGES;

create database user_db charset utf8 ;

use user_db ;

create table user ( id int auto_increment , name varchar(100)  , age smallint , height float , sex char(1) , comment text , birthday date , create_at timestamp , primary key (id) );

insert into user (name , age , height , sex , comment , birthday , create_at ) values ('wens' , 29 , 165.66, 'M' , '音乐 读书 数学 编程' , '1986-01-06' , now() ) ;
