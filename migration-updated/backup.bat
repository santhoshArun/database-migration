@echo off
d:
cd/
cd mysql/bin
net start mysql
e:
cd/
cd migration/src
javac -d . MigrationBackup.java
java -cp mysql-connector-java-5.1.47.jar;. migrator.MigrationBackup ../mig/mig_file.txt
pause