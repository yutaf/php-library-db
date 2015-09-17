<?php

/**
 * Class DbRepository
 */
namespace Yutaf;

abstract class DbRepository
{
    protected $con;
    protected $dbManager;
    protected $table_name;
    public $columns;

    abstract protected function setTableName();

    /**
     * コンストラクタ
     *
     * @param \PDO $con
     * @param DbManager $dbManager
     */
    public function __construct(\PDO $con, DbManager $dbManager)
    {
        $this->setConnection($con);
        $this->dbManager = $dbManager;
        $this->setTableName();
        $this->setColumns();
    }

    /**
     * コネクションを設定
     *
     * @param \PDO $con
     */
    public function setConnection($con)
    {
        $this->con = $con;
    }

    /**
     * カラム名を設定
     */
    protected function setColumns()
    {
        $this->columns = $this->getColumns();
    }

    /**
     * カラム名を取得
     *
     * @return array
     */
    protected function getColumns()
    {
        $sql = <<<EOQ
DESCRIBE {$this->table_name}
;
EOQ;
        return $this->fetchAll($sql, array(), \PDO::FETCH_COLUMN, 0);
    }

    /**
     * クエリを実行
     *
     * @param $sql
     * @param array $params
     * @return \PDOStatement $stmt
     */
    public function execute($sql, $params=array())
    {
        $stmt = $this->con->prepare($sql);
        $stmt->execute($params);

        return $stmt;
    }

    /**
     * クエリを実行し、結果を1行取得
     *
     * @param string $sql
     * @param array $params
     * @param $fetch_style
     * @return mixed
     */
    public function fetch($sql, $params=array(), $fetch_style=\PDO::FETCH_ASSOC)
    {
        return $this->execute($sql, $params)->fetch($fetch_style);
    }

    /**
     * クエリを実行し、結果をすべて取得
     *
     * @param null $sql
     * @param array $params
     * @param int $fetch_style
     * @param int $fetch_argument
     * @return array
     */
    public function fetchAll($sql=null, $params=array(), $fetch_style=\PDO::FETCH_ASSOC, $fetch_argument=0)
    {
        if(empty($sql)) $sql = 'SELECT * FROM '.$this->table_name;
        return ($fetch_style == \PDO::FETCH_COLUMN)? $this->execute($sql, $params)->fetchAll($fetch_style, $fetch_argument) : $this->execute($sql, $params)->fetchAll($fetch_style);
    }

    /**
     * insert
     *
     * @param $values
     * @throws \LogicException
     */
    public function insert($values)
    {
        if(! is_array($values) && count($values) === 0) {
            throw new \LogicException('invalid argument');
        }
        $insertData = $this->getInsertColumnsPlaceholdersParams($values);

        $sql = <<<EOQ
INSERT INTO {$this->table_name}
({$insertData['columns']})
VALUES({$insertData['placeholders']})
;
EOQ;

        $this->execute($sql, $insertData['params']);
    }

    /**
     * insertクエリに使用するカラム文、プレースホルダ文、パラメータを取得
     *
     * @param $values
     * @return array|bool
     * @throws \LogicException
     */
    protected function getInsertColumnsPlaceholdersParams($values)
    {
        if(! is_array($values) && count($values) === 0) {
            throw new \LogicException('invalid argument');
        }

        $returns = array(
            'columns' => '',
            'placeholders' => '',
            'params' => array()
        );
        $tmpColumns = array();
        $tmpPlaceholders = array();
        foreach($values as $column => $value) {
            if(! in_array($column, $this->columns)) {
                throw new \LogicException('Indicated column does not exist. :'.$column);
            }
            $tmpColumns[] = $column;
            $placeholder = ':'.$column;
            $tmpPlaceholders[] = $placeholder;
            $returns['params'][$placeholder] = $value;
        }

        $returns['columns'] = implode(', ', $tmpColumns);
        $returns['placeholders'] = implode(', ', $tmpPlaceholders);

        return $returns;
    }

    /**
     * 一度のクエリで複数レコードの insert を行う
     *
     * @param $values_set
     * @throws \LogicException
     */
    public function insertMultipleRows($values_set)
    {
        if(! is_array($values_set) || count($values_set) === 0) {
            throw new \LogicException('invalid argument');
        }

        // columns, placeholders を取得
        $insertData = $this->getMultipleInsertColumnsPlaceholdersParams($values_set);

        $sql = <<<EOQ
INSERT INTO {$this->table_name}
({$insertData['columns']})
VALUES
{$insertData['VALUES_string']}
;
EOQ;
        $this->execute($sql, $insertData['params']);
    }

    /**
     * insertMultipleRows メソッドで使用するsqlのパーツを取得
     *
     * @param $values_set
     * @return array
     * @throws \LogicException
     */
    public function getMultipleInsertColumnsPlaceholdersParams($values_set)
    {
        if(! is_array($values_set) && count($values_set) === 0) {
            throw new \LogicException('invalid argument');
        }

        // initialize
        $returns = array(
            'columns' => '',
            'VALUES_string' => '',
            'params' => array(),
        );

        $tmpColumns = array();
        $placeholders_set = array();
        $VALUES_array = array();

        $i_values_set = 0;

        foreach($values_set as $values) {
            // initialize
            $placeholders = array();

            foreach($values as $column => $value) {
                if(! in_array($column, $this->columns)) {
                    throw new \LogicException('Indicated column does not exist. :'.$column);
                }

                if($i_values_set === 0) {
                    $tmpColumns[] = $column;
                }


                $placeholder = ':'.$column.$i_values_set; // :foo0, :foo1, :foo2...

                $placeholders[] = $placeholder;
                $returns['params'][$placeholder] = $value;
            }

            $placeholders_implode = implode(',', $placeholders);
            $VALUES_array[] = '('.$placeholders_implode.')';

            $returns['columns'] = implode(', ', $tmpColumns);

            $i_values_set++;
        }

        $returns['VALUES_string'] = implode(',', $VALUES_array);

        return $returns;
    }

    /**
     * update by id
     *
     * @param $id
     * @param $values
     * @param array $wheres
     * @param array $params
     * @throws \LogicException
     */
    public function updateById($id, $values, $wheres=array(), $params=array())
    {
        if(array_key_exists('id', $values)) {
            unset($values['id']);
        }
        if(! is_array($values) || count($values) === 0) {
            throw new \LogicException("Error: Update Columns Are Incorrect. \n".var_export($values, true));
        }
        $updateData = $this->getUpdateSetsParams($values);

        $wheres[] = 'id=:id';
        $params[':id'] = $id;

        $where = 'WHERE '.implode(' AND ', $wheres);

        $sql = <<<EOQ
UPDATE {$this->table_name}
SET {$updateData['sets']}
{$where}
;
EOQ;
        $params = array_merge($params, $updateData['params']);
        $this->execute($sql, $params);
    }

    /**
     * 複数 id を指定して update
     *
     * @param array $ids
     * @param array $values
     * @throws \LogicException
     */
    public function updateByIds($ids=array(), $values=array())
    {
        if(array_key_exists('id', $values)) {
            unset($values['id']);
        }
        if(
            (! is_array($ids) || count($ids) === 0)
            || (! is_array($values) || count($values) === 0)
        ) {
            throw new \LogicException('invalid argument');
        }

        $updateData = $this->getUpdateSetsParams($values);
        $ids_implode = implode(',', $ids);

        $sql = <<<EOQ
UPDATE {$this->table_name}
SET {$updateData['sets']}
WHERE id IN({$ids_implode})
;
EOQ;
        $this->execute($sql, $updateData['params']);
    }

    /**
     * wheres, params を指定して update
     *
     * @param $values
     * @param $wheres
     * @param $params
     * @throws \LogicException
     */
    public function update($values, $wheres, $params)
    {
        if(
            ! is_array($values) || count($values)===0
            || ! is_array($wheres) || count($wheres)===0
            || ! is_array($params) || count($params)===0
        ) {
            throw new \LogicException('invalid argument.');
        }

        $updateData = $this->getUpdateSetsParams($values);

        $where = 'WHERE '.implode(' AND ', $wheres);

        $sql = <<<EOQ
UPDATE {$this->table_name}
SET {$updateData['sets']}
{$where}
;
EOQ;
        $params = array_merge($params, $updateData['params']);
        $this->execute($sql, $params);
    }

    /**
     * updateクエリに使用するSET文、パラメータを取得
     *
     * @param $values
     * @return array|bool
     * @throws \LogicException
     */
    protected function getUpdateSetsParams($values)
    {
        $returns = array(
            'sets' => '',
            'params' => array()
        );
        $tmpSets = array();

        foreach($values as $column => $value) {
            if(! in_array($column, $this->columns)) {
                throw new \LogicException('Indicated column does not exist. :'.$column);
            }
            $placeholder = ':'.$column;
            $tmpSets[] = $column.'='.$placeholder;
            $returns['params'][$placeholder] = $value;
        }

        if(count($returns['params']) === 0) {
            throw new \LogicException("Error: Update Columns Are Incorrect. \n".var_export($values, true));
        }
        $returns['sets'] = implode(', ', $tmpSets);

        return $returns;
    }

    /**
     * 全id取得
     *
     * @return array
     * @throws \LogicException
     */
    public function fetchAllIds()
    {
        if(! in_array('id', $this->columns)) {
            throw new \LogicException('"id" column does not exist.');
        }
        $sql = <<<EOQ
SELECT id FROM {$this->table_name}
;
EOQ;
        return $this->fetchAll($sql, array(), \PDO::FETCH_COLUMN);
    }

    /**
     * テーブルのレコード総数を取得
     *
     * @param string $where
     * @param array $params
     * @return int
     */
    public function fetchCnt($where='', $params=array())
    {
        $sql = <<<EOQ
SELECT COUNT(id) FROM {$this->table_name}
{$where}
;
EOQ;
        $result = $this->fetch($sql, $params, \PDO::FETCH_NUM);
        return $result[0];
    }

    /**
     * idを指定してレコードを取得
     *
     * @param $id
     * @return bool|mixed
     */
    public function fetchById($id)
    {
        $sql = <<<EOQ
SELECT * FROM {$this->table_name} WHERE id=:id;
EOQ;
        $result = $this->fetch($sql, array(':id' => $id));
        if(! $result) return false;
        return $result;
    }

    /**
     * 最新レコードを一件取得
     *
     * @return mixed
     * @throws \LogicException
     */
    public function fetchLatest()
    {
        if(! in_array('id', $this->columns)) {
            throw new \LogicException('"id" column does not exist.');
        }
        $sql = <<<EOQ
SELECT * FROM {$this->table_name}
ORDER BY id DESC
LIMIT 1
;
EOQ;
        return $this->fetch($sql);
    }

    /**
     * 最新idを一件取得
     *
     * @return bool
     * @throws \LogicException
     */
    public function fetchLatestId()
    {
        if(! in_array('id', $this->columns)) {
            throw new \LogicException('"id" column does not exist.');
        }
        $sql = <<<EOQ
SELECT id FROM {$this->table_name}
ORDER BY id DESC
LIMIT 1
;
EOQ;
        $result = $this->fetch($sql, array(), \PDO::FETCH_NUM);
        if(! $result) {
            return false;
        }
        return $result[0];
    }

    /**
     * 最古レコードを一件取得
     *
     * @return mixed
     * @throws \LogicException
     */
    public function fetchOldest()
    {
        if(! in_array('id', $this->columns)) {
            throw new \LogicException('"id" column does not exist.');
        }
        $sql = <<<EOQ
SELECT * FROM {$this->table_name}
ORDER BY id ASC
LIMIT 1
;
EOQ;
        return $this->fetch($sql);
    }

    /**
     * 最も過去の created を取得
     *
     * @return bool
     */
    public function fetchOldestCreated()
    {
        $sql = <<<EOQ
SELECT created FROM {$this->table_name} ORDER BY created ASC LIMIT 1
;
EOQ;
        $result = $this->fetch($sql, array(), \PDO::FETCH_NUM);
        if(! $result) {
            return false;
        }
        return $result[0];
    }

    /**
     * 最新の created を取得
     *
     * @return bool
     */
    public function fetchLatestCreated()
    {
        $sql = <<<EOQ
SELECT created FROM {$this->table_name} ORDER BY created DESC LIMIT 1
;
EOQ;
        $result = $this->fetch($sql, array(), \PDO::FETCH_NUM);
        if(! $result) {
            return false;
        }
        return $result[0];
    }

    /**
     * 条件を指定して id を取得
     *
     * @param array $conditions
     * @return bool
     */
    public function fetchIdByConditions($conditions=array())
    {
        if(! isset($conditions) || ! is_array($conditions) || count($conditions)===0) {
            return false;
        }
        $conditions['column'] = 'id';
        $sql_and_params = $this->getSqlAndParamsByConditions($conditions);
        if(! $sql_and_params) {
            return false;
        }
        $result = $this->fetch($sql_and_params['sql'], $sql_and_params['params'], \PDO::FETCH_NUM);
        if(! $result) {
            return false;
        }
        return $result[0];
    }

    /**
     * 条件を指定して全ての id を取得
     *
     * @param array $conditions
     * @return array|bool
     */
    public function fetchAllIdsByConditions($conditions=array())
    {
        if(! isset($conditions) || ! is_array($conditions) || count($conditions)===0) {
            return array();
        }
        $conditions['column'] = 'id';
        $sql_and_params = $this->getSqlAndParamsByConditions($conditions);
        if(! $sql_and_params) {
            return array();
        }
        return $this->fetchAll($sql_and_params['sql'], $sql_and_params['params'], \PDO::FETCH_COLUMN);
    }

    /**
     * getSqlAndParamsByConditions
     *
     * @param array $conditions
     * @return array|bool
     */
    private function getSqlAndParamsByConditions($conditions=array())
    {
        if(! isset($conditions) || ! is_array($conditions) || count($conditions)===0) {
            return false;
        }
        if(! isset($conditions['column']) || strlen($conditions['column'])===0) {
            return false;
        }

        $wheres = array();
        $params = array();
        if(isset($conditions['wheres']) && is_array($conditions['wheres'])>0 && count($conditions['wheres'])>0) {
            foreach($conditions['wheres'] as $k => $v) {
                if(is_array($v) && count($v)>0) {
                    $suffix = 0;
                    $placeholders = array();
                    foreach($v as $vv) {
                        $placeholders[] = ":{$k}{$suffix}";
                        $params[":{$k}{$suffix}"] = $vv;
                        $suffix++;
                    }
                    $implode_placeholders = implode(',', $placeholders);

                    $wheres[] = "{$k} IN({$implode_placeholders})";
                } else {
                    $wheres[] = "{$k}=:{$k}";
                    $params[":{$k}"] = $v;
                }
            }
        }
        if(isset($conditions['wheres_not']) && is_array($conditions['wheres_not'])>0 && count($conditions['wheres_not'])>0) {
            foreach($conditions['wheres_not'] as $k => $v) {
                if(is_array($v) && count($v)>0) {
                    $suffix = 0;
                    $placeholders = array();
                    foreach($v as $vv) {
                        $placeholders[] = ":{$k}{$suffix}";
                        $params[":{$k}{$suffix}"] = $vv;
                        $suffix++;
                    }
                    $implode_placeholders = implode(',', $placeholders);

                    $wheres[] = "{$k} NOT IN({$implode_placeholders})";
                } else {
                    $wheres[] = "{$k}!=:{$k}";
                    $params[":{$k}"] = $v;
                }
            }
        }

        $where = '';
        if(count($wheres)>0) {
            $where = 'WHERE '.implode(' AND ', $wheres);
        }

        $limit = '';
        if(isset($conditions['limit']) && is_numeric($conditions['limit'])) {
            $offset = '';
            if(isset($conditions['offset']) && is_numeric($conditions['offset'])) {
                $limit = "LIMIT {$conditions['offset']}, {$conditions['limit']}";
            } else {
                $limit = "LIMIT {$conditions['limit']}";
            }
        }

        $sql = <<<EOL
SELECT {$conditions['column']} FROM {$this->table_name} {$where} {$limit}
;
EOL;
        return array(
            'sql' => $sql,
            'params' => $params,
        );
    }

    /**
     * ランダムにidを取得
     *
     * @return bool
     * @throws \LogicException
     */
    public function fetchRandomId()
    {
        if(! in_array('id', $this->columns)) {
            throw new \LogicException('"id" column does not exist.');
        }

        $sql = <<<EOQ
SELECT id FROM {$this->table_name}
ORDER BY RAND()
LIMIT 1
;
EOQ;
        $result = $this->fetch($sql, array(), \PDO::FETCH_NUM);
        if(! $result) {
            return false;
        }

        return $result[0];
    }

    /**
     * ランダムにid群を取得
     *
     * @param $num
     * @return array
     * @throws \LogicException
     */
    /**
     * @param $num
     * @return array
     * @throws \LogicException
     */
    public function fetchAllRandomIds($num)
    {
        if(! in_array('id', $this->columns)) {
            throw new \LogicException('"id" column does not exist.');
        }

        $sql = <<<EOQ
SELECT id FROM {$this->table_name}
ORDER BY RAND()
LIMIT {$num}
;
EOQ;
        return $this->fetchAll($sql, array(), \PDO::FETCH_COLUMN);
    }

    /**
     * ランダムにレコードを取得
     *
     * @param $num
     * @param array $wheres
     * @param array $params
     * @return array
     */
    public function fetchAllRandomRows($num, $wheres=array(), $params=array())
    {
        if(is_array($wheres) && count($wheres)>0) {
            $where = 'WHERE '.implode(' AND ', $wheres);
        } else {
            $where = '';
        }

        $sql = <<<EOQ
SELECT * FROM {$this->table_name}
{$where}
ORDER BY RAND()
LIMIT {$num}
;
EOQ;
        return $this->fetchAll($sql, $params);
    }

    /**
     * id を配列で指定してレコードを削除
     *
     * @param array $ids
     * @throws \LogicException
     */
    public function deleteByIds($ids=array())
    {
        if(! is_array($ids) || count($ids)===0) {
            throw new \LogicException('invalid argument.: '.var_export($ids, true));
        }

        $ids_implode = implode(',', $ids);

        $sql = <<<EOQ
DELETE
FROM {$this->table_name}
WHERE id IN({$ids_implode})
;
EOQ;
        $this->execute($sql);
    }

    /**
     * レコードを１件削除する
     *
     * @param $id
     */
    public function delete($id)
    {
        if(! $this->fetchById($id)) return;
        $sql = <<<EOQ
DELETE
FROM {$this->table_name}
WHERE id=:id
;
EOQ;
        $this->execute($sql, array(':id' => $id));
    }

    /**
     * sortを取得
     *
     * @param $id
     * @return bool|int
     * @throws \LogicException
     */
    public function fetchSort($id)
    {
        if(! in_array('sort', $this->columns)) {
            throw new \LogicException('"sort" column does not exist.');
        }
        $sql = <<<EOQ
SELECT sort
FROM {$this->table_name}
WHERE id=:id
;
EOQ;
        $result = $this->fetch($sql, array(':id'=>$id), \PDO::FETCH_NUM);
        if(! $result) return false;
        return (int)$result[0];
    }

    /**
     * 指定カラム、order での順位を取得
     *
     * @param $column
     * @param $value
     * @param $order
     * @return int
     */
    protected function getRank($column, $value, $order)
    {
        $operator = ($order == 'DESC')? '>' : '<';

        $sql = <<<EOQ
SELECT COUNT(id) + 1
FROM {$this->table_name}
WHERE {$column} {$operator} :value
;
EOQ;
        $params = array(
            ':value' => $value
        );

        $result = $this->fetch($sql, $params, \PDO::FETCH_NUM);
        return (int)$result[0];
    }

    /**
     * テーブルのソート順をランダムに入れ替える
     *
     * @param null $cnt
     * @throws \LogicException
     */
    public function makeSortRandom($cnt=null)
    {
        if(! in_array('sort', $this->columns)) {
            throw new \LogicException('"sort" column does not exist.');
        }
        if(is_null($cnt)) {
            $cnt = $this->fetchCnt();
        }
        if($cnt === 0) {
            return;
        }
        $random_ids1 = $this->fetchAllRandomIds($cnt);
        $random_ids2 = $this->fetchAllRandomIds($cnt);

        for($i=0; $i<$cnt; $i++) {
            $this->replaceSort($random_ids1[$i], $random_ids2[$i]);
        }
    }

    /**
     * 指定id間の sort 値を入れ替える
     *
     * @param $id1
     * @param $id2
     * @throws \LogicException
     */
    public function replaceSort($id1, $id2)
    {
        if(! in_array('sort', $this->columns)) {
            throw new \LogicException('"sort" column does not exist.');
        }
        $row1 = $this->fetchById($id1);
        $row2 = $this->fetchById($id2);
        if(! $row1 || ! $row2) {
            return;
        }

        $this->updateById($row1['id'], array('sort' => $row2['sort']));
        $this->updateById($row2['id'], array('sort' => $row1['sort']));
    }

    /**
     * 直近のレコードとソート順を入れ替える
     *
     * @param $id
     * @param bool $up
     * @param string $where
     * @param array $params
     * @param string $join
     * @throws \LogicException
     */
    public function sortWithNearby($id, $up=true, $where='', $params=array(), $join='')
    {
        if(! in_array('sort', $this->columns)) {
            throw new \LogicException('"sort" column does not exist.');
        }
        if(! $row = $this->fetchById($id)) {
            return;
        }

        if(strlen($where) > 0) {
            $rowNearby = $this->fetchRowNearby($id, $up, $where, $params, $join);
        } else {
            $rowNearby = $this->fetchRowNearby($id, $up);
        }

        if(! $rowNearby) {
            return;
        }

        $this->updateById($row['id'], array('sort' => $rowNearby['sort']));
        $this->updateById($rowNearby['id'], array('sort' => $row['sort']));
    }

    /**
     * 直近のレコードを取得
     *
     * @param $id
     * @param bool $up
     * @param string $where_additional
     * @param array $params_additional
     * @param string $join
     * @return mixed
     * @throws \LogicException
     */
    protected function fetchRowNearby($id, $up=true, $where_additional='', $params_additional=array(), $join='')
    {
        if(! in_array('sort', $this->columns)) {
            throw new \LogicException('"sort" column does not exist.');
        }
        if($up) {
            $operator = '>';
            $order = 'ASC';
        } else {
            $operator = '<';
            $order = 'DESC';
        }

        $where_additional = (strlen($where_additional) > 0)? 'AND '.$where_additional : '';
        $sort = $this->fetchSort($id);
        $params = array_merge(
            $params_additional,
            array(':sort' => $sort)
        );

        $sql = $this->getSqlForFetchRowNearby($operator, $where_additional, $order, $join);
        return $this->fetch($sql, $params);
    }

    /**
     * self::fetchRowNearby 用の sql を取得
     * 必要に応じてカラム名の略称を、各 model でオーバーライドする
     *
     * @param $operator
     * @param $where_additional
     * @param $order
     * @param $join
     * @return string
     */
    protected function getSqlForFetchRowNearby($operator, $where_additional, $order, $join='')
    {
        return <<<EOQ
SELECT id, sort
FROM {$this->table_name}
{$join}
WHERE sort {$operator} :sort
{$where_additional}
ORDER BY sort {$order}
LIMIT 1
;
EOQ;
    }

    /**
     * タイトルを取得
     *
     * @param $id
     * @return bool
     * @throws \LogicException
     */
    public function fetchTitle($id)
    {
        if(! in_array('title', $this->columns)) {
            throw new \LogicException('"title" column does not exist.');
        }

        $sql = <<<EOQ
SELECT title FROM {$this->table_name} WHERE id=:id;
EOQ;
        $result = $this->fetch($sql, array(':id'=>$id), \PDO::FETCH_NUM);
        if(! $result) return false;
        return $result[0];
    }

    /**
     * 特定カラムの値を指定して件数を取得
     *
     * @param $column
     * @param $value
     * @return int
     */
    public function fetchCountByParticularColumn($column, $value)
    {
        $sql = <<<EOQ
SELECT COUNT(id) FROM {$this->table_name} WHERE {$column} = :column;
EOQ;
        $result = $this->fetch($sql, array(':column' => $value), \PDO::FETCH_NUM);
        return (int)$result[0];
    }

    /**
     * TRUNCATE
     */
    public function truncate()
    {
        $sql = <<<EOQ
TRUNCATE {$this->table_name}
;
EOQ;
        $this->execute($sql);
    }

    /**
     * 変数が整数かどうかを調べる
     *
     * @param $var
     * @return bool
     */
    public function isInt($var)
    {
        if(preg_match('/^[0-9][0-9]*$/', $var) === 1) {
            return true;
        }
        return false;
    }
}
