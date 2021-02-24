import environ


INI_PULSE = environ.secrets.INISecrets.from_path_in_env("APP_PULSE_SECRET")
INI_DB = environ.secrets.INISecrets.from_path_in_env("APP_DB_SECRET")


@environ.config(prefix="APP")
class AppConfig:
    """
    AppConfig Class representing the configuration of the application
    """

    @environ.config(prefix="PULSE")
    class AthenaConfig:
        """
        AthenaConfig class represeting the configuration to access
        pulse service
        """
        s3_bucket: str = INI_PULSE.secret(
            name="bucket", default=environ.var())
        user: str = INI_PULSE.secret(
            name="user", default=environ.var())
        access_key: str = INI_PULSE.secret(
            name="accesskey", default=environ.var())
        secret_key: str = INI_PULSE.secret(
            name="secretkey", default=environ.var())
        region: str = INI_PULSE.secret(
            name="region", default=environ.var())

    @environ.config(prefix="DB")
    class DBConfig:
        """
        DBConfig Class representing the configuration to access the database
        """
        host: str = INI_DB.secret(name="host", default=environ.var())
        port: int = INI_DB.secret(name="port", default=environ.var())
        name: str = INI_DB.secret(name="dbname", default=environ.var())
        user: str = INI_DB.secret(name="user", default=environ.var())
        password: str = INI_DB.secret(name="password", default=environ.var())
    athenaConf = environ.group(AthenaConfig)
    db = environ.group(DBConfig)


def getConf():
    return environ.to_config(AppConfig)
