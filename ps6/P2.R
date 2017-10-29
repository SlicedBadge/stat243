library(RSQLite)

drv <- dbDriver("SQLite")
dir = '../../'
dbFilename = 'stackoverflow-2016.db'
db = dbConnect(drv, dbname = file.path(dir, dbFilename))


dbListTables(db)
dbListFields(db, "users")

dbGetQuery(db, "create view Pyusers as select distinct userid from users U
                    join questions Q on U.userid = Q.ownerid
                    join questions_tags T on Q.questionid = T.questionid
                     where tag = 'python' ")

Rusers = dbGetQuery(db, "select distinct userid from users U join questions Q on U.userid = Q.ownerid
                    join questions_tags T on Q.questionid = T.questionid
                    where tag = 'r' and userid not in Pyusers")
