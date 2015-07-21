<?php

/**
 * Class DbManager
 *
 * @author Yuta Fujishiro<fujishiro@amaneku.co.jp>
 */

class DbManager
{
    protected $con;
    protected $repositories = array();

    /**
     * データベースへ接続
     */
    public function connect()
    {
        $options = array();
        $dsn = 'mysql:dbname='.DATABASE_NAME.';host='.DATABASE_HOST;
        if (version_compare(PHP_VERSION, '5.3.6') >= 0) {
            $dsn .= ';charset='.ENCODING_DB;
        } else {
            $options = array_merge($options, array(
                PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES `'.ENCODING_DB.'`',
            ));
        }
        if(defined('DATABASE_UNIX_SOCKET')) {
            $dsn .= ';unix_socket='.DATABASE_UNIX_SOCKET;
        }

        if(defined('MYSQL_TIMEZONE_ADJUST')) {
            $options = array_merge($options, array(
                PDO::MYSQL_ATTR_INIT_COMMAND => 'SET time_zone = `'.MYSQL_TIMEZONE_ADJUST.'`',
                // quit this because of causing warning
                // Warning: PDO::__construct(): MySQL server has gone away in ....
//                PDO::ATTR_PERSISTENT => true,
            ));
        }

        $this->con = new PDO(
            $dsn,
            DATABASE_USER,
            DATABASE_PASSWORD,
            $options
        );
        $this->con->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
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
        if(method_exists('PDO', 'inTransaction')) {
            return $this->con->inTransaction();
        }
        return false;
    }
}
