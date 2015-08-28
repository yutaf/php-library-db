<?php

/**
 * Class DbManager
 */
namespace Yutaf;

class DbManager
{
    protected $con;
    protected $repositories = array();

    /**
     * データベースへ接続
     *
     * @param array $config
     */
    public function connect($config=array())
    {
        $options = array();
        $dsn = 'mysql:dbname='.$config['dbname'].';host='.$config['host'];

        if(isset($config['charset']) && strlen($config['charset'])>0) {
            if (version_compare(PHP_VERSION, '5.3.6') >= 0) {
                $dsn .= ';charset='.$config['charset'];
            } else {
                $options = array_merge($options, array(
                    \PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES `'.$config['charset'].'`',
                ));
            }
        }

        if(isset($config['unix_socket']) && strlen($config['unix_socket'])>0) {
            $dsn .= ';unix_socket='.$config['unix_socket'];
        }

        if(isset($config['time_zone']) && strlen($config['time_zone'])>0) {
            $options = array_merge($options, array(
                \PDO::MYSQL_ATTR_INIT_COMMAND => 'SET time_zone = `'.$config['time_zone'].'`',
                // quit this because of causing warning
                // Warning: \PDO::__construct(): MySQL server has gone away in ....
//                \PDO::ATTR_PERSISTENT => true,
            ));
        }

        $this->con = new \PDO(
            $dsn,
            $config['username'],
            $config['password'],
            $options
        );
        $this->con->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
    }

    /**
     * リポジトリを取得
     *
     * @param string $repository_name
     * @return DbRepository
     */
    public function get($repository_name)
    {
        if (!isset($this->repositories[$repository_name])) {
            $repository_class = $repository_name . 'Repository';
            $repository = new $repository_class($this->con, $this);

            $this->repositories[$repository_name] = $repository;
        }

        return $this->repositories[$repository_name];
    }

    /**
     * デストラクタ
     * リポジトリと接続を破棄する
     */
    public function __destruct()
    {
        foreach ($this->repositories as $repository) {

            unset($repository);
        }

        unset($this->con);
    }

    public function getLastInsertId()
    {
        return $this->con->lastInsertId();
    }

    public function isConnected()
    {
        if(isset($this->con)) return true;
        return false;
    }

    public function beginTransaction()
    {
        $this->con->beginTransaction();
    }

    public function commit()
    {
        $this->con->commit();
    }

    public function rollBack()
    {
        $this->con->rollBack();
    }

    public function inTransaction()
    {
        if(method_exists('\PDO', 'inTransaction')) {
            return $this->con->inTransaction();
        }
        return false;
    }
}
