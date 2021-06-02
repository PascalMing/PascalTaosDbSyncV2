Taos数据库同步服务

功能：
   实现Taos不同数据库之间数据复制，可以用于数据库迁移和备份，支持续传

运行环境：JDK 1.8
          启动方法：java -jar Pascal.TaosDbSync.V2-1.0.jar

使用方法：
    1、在application.yml中配置正确的taos源数据库和目标数据库以及相关参数
    2、iot.servermode为false时采用进程模式，所有参数来自与.yml，执行完毕退出
    3、iot.servermode为true时，采用服务模式Web+RestAPI运行，执行后服务不退出。服务模式下，同步是时间范围和超级表（支持缺省值）由参数带入
       系统有简单的Index.html使用示例，使用方法
        1）选择同步数据的开始时间
        2）选择同步数据的时长和时间单位，数据范围 [开始时间,开始时间+时长)
        3）执行同步
        执行过程会动态显示

