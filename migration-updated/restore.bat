@echo off
e:
cd/
cd pstg/bin
net start postgresql-x64-9.6
e:
cd/
cd migration/src
javac -d . MigrationRestore.java
java -cp postgresql-42.2.5.jar;. migrator.MigrationRestore ../mig/mig_file.txt
pause