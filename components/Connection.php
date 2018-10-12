<?php

namespace pvsaintpe\db\components;

/**
 * Class Connection
 * @package pvsaintpe\db\components
 * @method Command|\yii\db\Command createCommand($sql = null, $params = [])
 */
class Connection extends \yii\db\Connection
{
    /**
     * @var string
     */
    private $dbName;

    /**
     * @return bool|string
     */
    public function getName()
    {
        if (!$this->dbName) {
            parse_str(str_replace(';', '&', substr(strstr($this->dsn, ':'), 1)), $dsn);
            if (!array_key_exists('host', $dsn)
                || !array_key_exists('port', $dsn)
                || !array_key_exists('dbname', $dsn)
            ) {
                $this->dbName = null;
            } else {
                $this->dbName = $dsn['dbname'];
            }
        }
        return $this->dbName;
    }

    /**
     * @param string $conditions
     * @param array $params
     * @return false|null|string
     * @throws \yii\db\Exception
     */
    public function selectScalar($conditions = '', $params = [])
    {
        return $this->createCommand($conditions, $params)->queryScalar();
    }

    /**
     * @param string $conditions
     * @param array $params
     * @param integer $fetchMode
     * @return false|null|string
     * @throws \yii\db\Exception
     */
    public function selectOne($conditions = '', $params = [], $fetchMode = null)
    {
        return $this->createCommand($conditions, $params)->queryOne($fetchMode);
    }

    /**
     * @param string $conditions
     * @param array $params
     * @return array
     * @throws \yii\db\Exception
     */
    public function selectColumn($conditions = '', $params = [])
    {
        return $this->createCommand($conditions, $params)->queryColumn();
    }

    /**
     * @param string $conditions
     * @param array $params
     * @param integer $fetchMode
     * @return array
     * @throws \yii\db\Exception
     */
    public function selectAll($conditions = '', $params = [], $fetchMode = null)
    {
        return $this->createCommand($conditions, $params)->queryAll($fetchMode);
    }

    /**
     * @param string $table
     * @param array $columns
     * @throws \yii\db\Exception
     * @return int
     */
    public function insert($table, $columns)
    {
        return $this->createCommand()->insert($table, $columns)->execute();
    }

    /**
     * @param string $table
     * @param array $columns
     * @param array $rows
     * @throws \yii\db\Exception
     */
    public function batchInsert($table, $columns, $rows)
    {
        $this->createCommand()->batchInsert($table, $columns, $rows)->execute();
    }

    /**
     * @param string $table
     * @param array $columns
     * @param array $condition
     * @throws \yii\db\Exception
     */
    public function batchUpdate($table, $columns, $condition)
    {
        $this->createCommand()->batchUpdate($table, $columns, $condition)->execute();
    }

    /**
     * @param string $sql
     * @param array $params
     * @throws \yii\db\Exception
     */
    public function execute($sql, $params = [])
    {
        $this->createCommand($sql)->bindValues($params)->execute();
    }

    /**
     * @param string $table
     * @param array $columns
     * @param string $condition
     * @param array $params
     * @throws \yii\db\Exception
     */
    public function update($table, $columns, $condition = '', $params = [])
    {
        $this->createCommand()->update($table, $columns, $condition, $params)->execute();
    }
}
