<?php

namespace pvsaintpe\db\components;

use yii\db\TableSchema;
use pvsaintpe\boost\db\Migration as BaseMigration;

/**
 * Class Migration
 * @package pvsaintpe\db\components
 * @property Connection $db
 * @method Connection getDb()
 */
class Migration extends BaseMigration
{
    public $logTableTriggerInit = false;

    /**
     * @param string $conditions
     * @param array $params
     * @return false|null|string
     * @throws \yii\db\Exception
     */
    public function selectScalar($conditions = '', $params = [])
    {
        return $this->db->createCommand($conditions, $params)->queryScalar();
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
        return $this->db->createCommand($conditions, $params)->queryOne($fetchMode);
    }

    /**
     * @param string $conditions
     * @param array $params
     * @return array
     * @throws \yii\db\Exception
     */
    public function selectColumn($conditions = '', $params = [])
    {
        return $this->db->createCommand($conditions, $params)->queryColumn();
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
        return $this->db->createCommand($conditions, $params)->queryAll($fetchMode);
    }

    /**
     * @param string $table
     * @param array $columns
     * @throws \yii\db\Exception
     */
    public function insert($table, $columns)
    {
        $this->db->createCommand()->insert($table, $columns)->execute();
    }

    /**
     * @param string $table
     * @param array $columns
     * @param array $rows
     * @throws \yii\db\Exception
     */
    public function batchInsert($table, $columns, $rows)
    {
        $this->db->createCommand()->batchInsert($table, $columns, $rows)->execute();
    }

    /**
     * @param string $table
     * @param array $columns
     * @param array $condition
     * @throws \yii\db\Exception
     */
    public function batchUpdate($table, $columns, $condition)
    {
        $this->db->createCommand()->batchUpdate($table, $columns, $condition)->execute();
    }

    /**
     * @param string $sql
     * @param array $params
     * @throws \yii\db\Exception
     */
    public function execute($sql, $params = [])
    {
        $this->db->createCommand($sql)->bindValues($params)->execute();
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
        $this->db->createCommand()->update($table, $columns, $condition, $params)->execute();
    }

    /**
     * @param string $name
     * @param string $sql
     * @throws \yii\db\Exception
     */
    public function createView($name, $sql)
    {
        $this->dropView($name);
        $this->execute("CREATE VIEW `{$name}` AS {$sql}");
    }

    /**
     * @param string $name
     * @throws \yii\db\Exception
     */
    public function dropView($name)
    {
        $this->execute("DROP VIEW IF EXISTS `{$name}`;");
    }

    /**
     * @param string $tableName
     * @param string $name
     * @param string $sql
     * @param string $event enum(
     *      'BEFORE INSERT',
     *      'BEFORE UPDATE',
     *      'BEFORE_DELETE',
     *      'AFTER INSERT',
     *      'AFTER UPDATE',
     *      'AFTER DELETE',
     * )
     * @param string $definer CURRENT_USER
     * @param boolean $leavePrefixEnable
     * @param string $dbPrefix
     * @throws \yii\db\Exception
     */
    public function createTrigger(
        $tableName,
        $name,
        $sql,
        $event = 'BEFORE INSERT',
        $definer = null,
        $leavePrefixEnable = false,
        $dbPrefix = null
    ) {
        if ($definer === null) {
            $definer = 'CURRENT_USER';
        }
        if (($dbName = $this->db->getName()) !== false) {
            $leaveSuffix = '';
            $leavePrefix = '';
            $definerPrefix = '';
            if (YII_ENV_TEST && !$this->logTableTriggerInit) {
                $leaveSuffix = "
                    IF (@TRIGGER_CHECKS = FALSE) THEN
                      LEAVE thisTrigger;
                    END IF;
                ";
                $leavePrefixEnable = true;
            }
            if ($leavePrefixEnable) {
                $leavePrefix = 'thisTrigger:';
            }
            if ($definer) {
                $definerPrefix = "DEFINER = {$definer}";
            }

            $dbName = $dbPrefix ?: $dbName;
            $this->dropTrigger($name);
            $this->execute("
                CREATE {$definerPrefix}
                TRIGGER `{$dbName}`.`{$name}`
                {$event} ON `{$dbName}`.`{$tableName}`
                FOR EACH ROW
                {$leavePrefix} BEGIN {$leaveSuffix}
                    {$sql}
                END
            ");
        }
    }

    /**
     * @param string $name
     * @param string $sql
     * @param array $params @example [['type' => 'IN', 'name' => 'arg_name', 'format' => 'INT UNSIGNED'], ...]
     * @param string $definer CURRENT_USER
     * @param array $options @example ['NOT DETERMINISTIC', 'CONTAINS SQL', 'SQL SECURITY DEFINER']
     * @param string $dbPrefix
     * @throws \yii\db\Exception
     */
    public function createProcedure($name, $sql, $params = [], $definer = 'CURRENT_USER', $options = [], $dbPrefix = null)
    {
        if (($dbName = $this->db->getName()) !== false) {
            $args = array_map(
                function ($item) {
                    return join(' ', [
                        $item['type'],
                        $item['name'],
                        $item['format'],
                    ]);
                },
                $params
            );
            $dbName = $dbPrefix ?: $dbName;
            $this->dropProcedure($name);
            $this->execute("
                CREATE DEFINER = {$definer}
                PROCEDURE {$dbName}.{$name}(" . implode(', ', $args) . ")
                " . join(' ', $options) . "
                BEGIN
                    {$sql}
                END
            ");
        }
    }

    /**
     * @param string $name
     * @param string $dbPrefix
     * @throws \yii\db\Exception
     */
    public function dropProcedure($name, $dbPrefix = null)
    {
        if (($dbName = $this->db->getName()) !== false) {
            $dbName = $dbPrefix ?: $dbName;
            $this->execute("DROP PROCEDURE IF EXISTS `{$dbName}`.`{$name}`");
        }
    }

    /**
     * @param string $name
     * @param string $dbPrefix
     * @throws \yii\db\Exception
     */
    public function dropTrigger($name, $dbPrefix = null)
    {
        if (($dbName = $this->db->getName()) !== false) {
            $dbName = $dbPrefix ?: $dbName;
            $this->execute("DROP TRIGGER IF EXISTS `{$dbName}`.`{$name}`");
        }
    }

    /**
     * @param string $table
     * @param string $column
     * @param string $type
     */
    public function alterColumn($table, $column, $type)
    {
        parent::alterColumn($table, $column, $this->fixColumnType($type));
    }

    /**
     * @param string $tableName
     * @param string $columnName
     * @return false|null|string
     * @throws \yii\db\Exception
     */
    public function hasColumn($tableName, $columnName)
    {
        return $this->db->existColumn($tableName, $columnName);
    }

    /**
     * @param string $table
     * @param string $column
     * @param \yii\db\ColumnSchemaBuilder|string $type
     */
    public function addColumn($table, $column, $type)
    {
        parent::addColumn($table, $column, $type);
    }

    /**
     * @param string $table
     * @param string $column
     */
    public function dropColumn($table, $column)
    {
        $tableColumns = $this->db->getTableSchema($table)->columnNames;
        if (!in_array($column, $tableColumns)) {
            return;
        }
        $fks = $this->getForeignNamesColumns($this->db->getTableSchema($table));
        if ($name = array_search($column, $fks)) {
            $this->dropForeignKey($name, $table);
        }
        parent::dropColumn($table, $column);
    }

    /**
     * @param string $table
     * @param array $columns
     */
    public function dropColumns($table, array $columns)
    {
        foreach ($columns as $column) {
            $this->dropColumn($table, $column);
        }
    }

    /**
     * @param null|string $name
     * @param string $table
     * @param string|array $columns
     * @param string $refTable
     * @param string|array $refColumns
     * @param null $delete
     * @param null $update
     */
    public function addForeignKey($name, $table, $columns, $refTable, $refColumns, $delete = null, $update = null)
    {
        parent::addForeignKey($name, $table, $columns, $refTable, $refColumns, $delete, $update);
    }

    /**
     * @param string $name
     * @param string $table
     */
    public function dropForeignKey($name, $table)
    {
        parent::dropForeignKey($name, $table);
    }

    /**
     * @param string $table
     * @param string $name
     * @param string $newName
     */
    public function renameColumn($table, $name, $newName)
    {
        parent::renameColumn($table, $name, $newName);
    }

    /**
     * @param string $table
     */
    public function dropTable($table)
    {
        parent::dropTable($table);
    }

    /**
     * @param string $tableName
     * @throws \yii\db\Exception
     */
    protected function createTriggerDeleteRestrictions($tableName)
    {
        $this->createTrigger(
            $tableName,
            $tableName . "_BEFORE_DELETE",
            /** @lang SQL */
            "
    SIGNAL SQLSTATE VALUE '03999'
    SET MESSAGE_TEXT = 'Delete operations are restricted.', MYSQL_ERRNO = 999;
            ",
            'BEFORE DELETE'
        );
    }

    /**
     * @param string $tableName
     * @throws \yii\db\Exception
     */
    protected function createTriggerUpdateRestrictions($tableName)
    {
        $this->createTrigger(
            $tableName,
            $tableName . "_BEFORE_UPDATE",
            /** @lang SQL */
            "
    SIGNAL SQLSTATE VALUE '03999'
    SET MESSAGE_TEXT = 'Update operations are restricted.', MYSQL_ERRNO = 998;
            ",
            'BEFORE UPDATE'
        );
    }

    /**
     * Получает имена колонок с внешнем ключом
     * ```php
     * [
     *  nameFk => column,
     *  ....
     * ]
     * ```
     *
     * @param TableSchema $tableSchema
     * @return array
     */
    private function getForeignNamesColumns(TableSchema $tableSchema)
    {
        return array_map(function ($foreignKey) {
            return array_filter(array_keys($foreignKey))[1];
        }, $tableSchema->foreignKeys);
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
        return $this->db->getColumns($tableName, $restrict);
    }

    /**
     * Получить уникальные ключи (без PRIMARY)
     * @param string $tableName
     * @return array
     * @throws \yii\db\Exception
     */
    public function getUniqueKeys($tableName)
    {
        return $this->db->getUniqueKeys($tableName);
    }

    /**
     * Получить обычные ключи/индексы
     * @param string $tableName
     * @return array
     * @throws \yii\db\Exception
     */
    public function getKeys($tableName)
    {
        return $this->db->getKeys($tableName);
    }

    /**
     * Получить PRIMARY ключи в таблице $tableName
     * @param string $tableName
     * @return array
     * @throws \yii\db\Exception
     */
    public function getPrimaryKeys($tableName)
    {
        return $this->db->getPrimaryKeys($tableName);
    }

    /**
     * Проверяет наличие таблицы $tableName
     * @param string $tableName
     * @return false|null|string
     * @throws \yii\db\Exception
     */
    public function existTable($tableName)
    {
        return $this->db->existTable($tableName);
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
        return $this->db->existColumn($tableName, $columnName);
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
        return $this->db->existKey($tableName, $key);
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
        return $this->db->existKeyOnColumn($tableName, $columnName);
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
        $this->db->cloneTable($tableName, $sourceTable, $sourceSchema);
    }
}
