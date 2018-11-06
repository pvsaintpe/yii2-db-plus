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
}
