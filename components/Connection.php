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

    /**
     * Получить расширенную информацию о колонках $tableName, исключая $restrict-columns
     * @param string $tableName
     * @param array $restrict
     * @return array
     * @throws \yii\db\Exception
     */
    public function getColumns($tableName, $restrict = [])
    {
        $sql = "SHOW FULL COLUMNS FROM `{$tableName}`";
        if ($restrict) {
            $sql .= " WHERE Field NOT IN ('" . join("','", $restrict) . "')";
        }
        return $this->createCommand($sql)->queryAll();
    }

    /**
     * Получить уникальные ключи (без PRIMARY)
     * @param string $tableName
     * @return array
     * @throws \yii\db\Exception
     */
    public function getUniqueKeys($tableName)
    {
        return $this->createCommand("
            SHOW KEYS FROM `{$tableName}`
            WHERE Key_name NOT LIKE 'PRIMARY' AND Non_unique LIKE 0
        ")->queryAll();
    }

    /**
     * @param $tableName
     * @return array
     */
    public function getForeignKeys($tableName)
    {
        return $this->getTableSchema($tableName) ? $this->getTableSchema($tableName)->foreignKeys : [];
    }

    /**
     * Получить обычные ключи/индексы
     * @param string $tableName
     * @return array
     * @throws \yii\db\Exception
     */
    public function getKeys($tableName)
    {
        return $this->createCommand("
            SHOW KEYS FROM `{$tableName}`
            WHERE Key_name NOT LIKE 'PRIMARY' AND Non_unique LIKE 1
        ")->queryAll();
    }

    /**
     * Получить PRIMARY ключи в таблице $tableName
     * @param string $tableName
     * @return array
     * @throws \yii\db\Exception
     */
    public function getPrimaryKeys($tableName)
    {
        return $this->createCommand("SHOW KEYS FROM `{$tableName}` WHERE Key_name LIKE 'PRIMARY'")->queryAll();
    }

    /**
     * Проверяет наличие таблицы $tableName
     * @param string $tableName
     * @return false|null|string
     * @throws \yii\db\Exception
     */
    public function existTable($tableName)
    {
        return $this->createCommand("SHOW TABLES LIKE '{$tableName}'")->queryScalar();
    }

    /**
     * Проверяет наличие столбца $columnName в таблице $tableName
     * @param string $tableName
     * @param string $columnName
     * @return false|null|string
     * @throws \yii\db\Exception
     */
    public function existColumn($tableName, $columnName)
    {
        return $this->createCommand("
            SHOW FULL COLUMNS FROM `{$tableName}` WHERE Field LIKE '{$columnName}'
        ")->queryScalar();
    }

    /**
     * Проверяет наличие ключа/индекса $key в таблице $tableName
     * @param string $tableName
     * @param string $key
     * @return false|null|string
     * @throws \yii\db\Exception
     */
    public function existKey($tableName, $key = 'PRIMARY')
    {
        return $this->createCommand("SHOW KEYS FROM `{$tableName}` WHERE Key_name LIKE '{$key}'")->queryScalar();
    }

    /**
     * Проверяет наличие ключа/индекса к столбцу $columnName в таблице $tableName
     * @param string $tableName
     * @param string $columnName
     * @return array|false
     * @throws \yii\db\Exception
     */
    public function existKeyOnColumn($tableName, $columnName)
    {
        return $this->createCommand("SHOW KEYS FROM `{$tableName}` WHERE Column_name LIKE '{$columnName}'")->queryAll();
    }

    /**
     * Создает таблицу $tableName клонируя структуру таблицы $sourceTable в $sourceSchema
     * @param string $tableName Целевое имя таблицы
     * @param string $sourceTable Имя таблицы источника
     * @param string|null $sourceSchema Имя схемы источника
     * @throws \yii\db\Exception
     * @return void
     */
    public function cloneTable($tableName, $sourceTable, $sourceSchema = null)
    {
        $source = "`{$sourceTable}`";
        if ($sourceSchema) {
            $source = "`{$sourceSchema}`.{$source}";
        }
        $this->execute("CREATE TABLE `{$tableName}` LIKE {$source}");
    }
}
