<?php

namespace pvsaintpe\db\components;

use pvsaintpe\db\components\mysql\Schema;
use pvsaintpe\db\components\TableSchema;
use Yii;

/**
 * Class Connection
 *
 * @package pvsaintpe\db\components
 *
// * @method Schema|\yii\db\Schema getSchema()
// * @method TableSchema|\yii\db\TableSchema getTableSchema($name, $refresh = false)
 */
class Connection extends \yii\db\Connection
{
    /**
     * @var string the class used to create new database [[Command]] objects.
     * If you want to extend the [[Command]] class,
     * you may configure this property to use your extended version of the class.
     * Since version 2.0.14 [[$commandMap]] is used if this property is set to its default value.
     * @see createCommand
     * @since 2.0.7
     * @deprecated since 2.0.14. Use [[$commandMap]] for precise configuration.
     */
    public $commandClass = 'pvsaintpe\db\components\Command';

    /**
     * @var array mapping between PDO driver names and [[Schema]] classes.
     * The keys of the array are PDO driver names while the values are either the corresponding
     * schema class names or configurations. Please refer to [[Yii::createObject()]] for
     * details on how to specify a configuration.
     *
     * This property is mainly used by [[getSchema()]] when fetching the database schema information.
     * You normally do not need to set this property unless you want to use your own
     * [[Schema]] class to support DBMS that is not supported by Yii.
     */
    public $schemaMap = [
        'pgsql' => 'yii\db\pgsql\Schema', // PostgreSQL
        'mysqli' => 'pvsaintpe\db\components\mysql\Schema', // MySQL
        'mysql' => 'pvsaintpe\db\components\mysql\Schema', // MySQL
        'sqlite' => 'yii\db\sqlite\Schema', // sqlite 3
        'sqlite2' => 'yii\db\sqlite\Schema', // sqlite 2
        'sqlsrv' => 'yii\db\mssql\Schema', // newer MSSQL driver on MS Windows hosts
        'oci' => 'yii\db\oci\Schema', // Oracle driver
        'mssql' => 'yii\db\mssql\Schema', // older MSSQL driver on MS Windows hosts
        'dblib' => 'yii\db\mssql\Schema', // dblib drivers on GNU/Linux (and maybe other OSes) hosts
        'cubrid' => 'yii\db\cubrid\Schema', // CUBRID
    ];

    /**
     * @var array mapping between PDO driver names and [[Command]] classes.
     * The keys of the array are PDO driver names while the values are either the corresponding
     * command class names or configurations. Please refer to [[Yii::createObject()]] for
     * details on how to specify a configuration.
     *
     * This property is mainly used by [[createCommand()]] to create new database [[Command]] objects.
     * You normally do not need to set this property unless you want to use your own
     * [[Command]] class or support DBMS that is not supported by Yii.
     * @since 2.0.14
     */
    public $commandMap = [
        'pgsql' => 'yii\db\Command', // PostgreSQL
        'mysqli' => 'pvsaintpe\db\components\Command', // MySQL
        'mysql' => 'pvsaintpe\db\components\Command', // MySQL
        'sqlite' => 'yii\db\sqlite\Command', // sqlite 3
        'sqlite2' => 'yii\db\sqlite\Command', // sqlite 2
        'sqlsrv' => 'yii\db\Command', // newer MSSQL driver on MS Windows hosts
        'oci' => 'yii\db\Command', // Oracle driver
        'mssql' => 'yii\db\Command', // older MSSQL driver on MS Windows hosts
        'dblib' => 'yii\db\Command', // dblib drivers on GNU/Linux (and maybe other OSes) hosts
        'cubrid' => 'yii\db\Command', // CUBRID
    ];

    /**
     * @var string
     */
    private $dbName;

    /**
     * @param string $name
     * @param bool $refresh
     * @return \yii\db\TableSchema|TableSchema
     */
    public function getTableSchema($name, $refresh = false)
    {
        return parent::getTableSchema($name, $refresh);
    }

    /**
     * @return \yii\db\Schema|Schema
     */
    public function getSchema()
    {
        return parent::getSchema();
    }

    /**
     * Creates a command for execution.
     * @param string $sql the SQL statement to be executed
     * @param array $params the parameters to be bound to the SQL statement
     * @return Command the DB command
     */
    public function createCommand($sql = null, $params = [])
    {
        $driver = $this->getDriverName();
        $config = ['class' => 'pvsaintpe\db\components\Command'];
        if ($this->commandClass !== $config['class']) {
            $config['class'] = $this->commandClass;
        } elseif (isset($this->commandMap[$driver])) {
            $config = !is_array($this->commandMap[$driver]) ? ['class' => $this->commandMap[$driver]] : $this->commandMap[$driver];
        }
        $config['db'] = $this;
        $config['sql'] = $sql;
        /** @var Command $command */
        $command = Yii::createObject($config);
        return $command->bindValues($params);
    }

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
        return $this->createCommand(
            "SHOW KEYS FROM `{$tableName}`WHERE Key_name NOT LIKE 'PRIMARY' AND Non_unique LIKE 0"
        )->queryAll();
    }

    /**
     * Получить первый уникальный ключ
     * @param string $tableName
     * @return string
     * @throws \yii\db\Exception
     */
    public function getFirstUniqueKey($tableName)
    {
        $uniqueKeys = $this->createCommand(
            "SHOW KEYS FROM `{$tableName}`WHERE Key_name NOT LIKE 'PRIMARY' AND Non_unique LIKE 0"
        )->queryAll();

        $firstKey = null;
        foreach ($uniqueKeys as $uniqueKey) {
            if (!$firstKey) {
                $firstKey = $uniqueKey['Column_name'];
            }
            $column = $this->getTableSchema($tableName)->getColumn($uniqueKey['Column_name']);
            if ($column->name == $uniqueKey['Column_name'] && in_array($column->type, [
                    Schema::TYPE_STRING,
                    Schema::TYPE_CHAR,
                    Schema::TYPE_TEXT,
                ])) {
                return $column->name;
            }
        }
        return $firstKey;
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

    /**
     * Call procedure
     * @param $name
     * @param array $params
     * @param int|null $fetchMode
     * @throws
     */
    public function call($name, array $params = [], $fetchMode = null)
    {
        $arguments = join(', :', array_keys($params));
        $this->execute("CALL $name(:{$arguments})", $params);
    }

    /**
     * @param string $table
     * @param array $where
     * @param array $replace
     * @param array $remove
     * @throws
     */
    public function insertFrom($table, $where, $replace, $remove = [])
    {
        $alias = 'w';

        /** @var TableSchema $tableSchema */
        $tableSchema = $this->getTableSchema($table);

        $columns = array_diff($tableSchema->getColumnNames(), $remove);

        $values = [];
        foreach ($columns as $column) {
            if (array_key_exists($column, $replace)) {
                $values[] = $this->quoteValue($replace[$column]);
                continue;
            }
            $values[] = $alias . '.' . $column;
        }

        $conditions = [];
        foreach ($where as $attribute => $condition) {
            if (!is_array($condition)) {
                $conditions[] = $alias . '.' . $attribute . ' = ' . $this->quoteValue($condition);
            } else {
                if (count($condition) > 0) {
                    $conditions[] = $alias . '.' . $attribute . ' IN (' . implode(', ', $condition) . ')';
                }
            }
        }

        $sql = "
            INSERT INTO `{$table}` (" . join(', ', $columns) . ")
            SELECT " . join(', ', $values) . "
            FROM `{$table}` `{$alias}`
            WHERE " . join(' AND ', $conditions) . "
        ";

        $this->execute($sql);
    }
}
