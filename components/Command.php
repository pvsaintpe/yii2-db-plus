<?php

namespace pvsaintpe\db\components;

use yii\db\Expression;
use yii\db\Query;
use yii\helpers\ArrayHelper;

/**
 * Class Command
 * @package pvsaintpe\db\components
 */
class Command extends \yii\db\Command
{
    /**
     * Example
     *
     * ```php
     * $this->batchUpdate(
     *      $tableName,
     *      [
     *          'name' => ['Alice', 'Bob'],
     *          'age' => '18'
     *      ],
     *      [
     *          'id' => [1, 2, 3],
     *          'enabled' => '1'
     *      ]
     * );
     * ```
     *
     * @param string $table
     * @param array $columns
     * @param array $condition
     * @return $this the command object itself
     */
    public function batchUpdate($table, array $columns, $condition)
    {
        if (($tableSchema = $this->db->getTableSchema($table)) !== null) {
            $columnSchemas = $tableSchema->columns;
        } else {
            $columnSchemas = [];
        }

        $lines = [];
        foreach ($columns as $name => $value) {
            if ($value instanceof Expression) {
                $lines[] = $this->db->quoteColumnName($name) . '=' . $value->expression;
            } elseif (is_array($value)) {
                $line = $this->db->quoteColumnName($name) . " = (CASE ";
                foreach ($value as $valueIndex => $val) {
                    $line .= " WHEN ";
                    $when = [];
                    foreach ($condition as $whenKey => $conditions) {
                        $param = (is_array($conditions) && isset($conditions[$valueIndex]))
                            ? $conditions[$valueIndex]
                            : $conditions;
                        $whenValue = !is_array($param) && isset($columnSchemas[$whenKey])
                            ? $columnSchemas[$whenKey]->dbTypecast($param)
                            : $param;
                        $when[] = $this->db->quoteColumnName($whenKey)
                            . " = "
                            . $this->db->quoteValue($whenValue);
                    }
                    $conditionValue = !is_array($val) && isset($columnSchemas[$name])
                        ? $columnSchemas[$name]->dbTypecast($val)
                        : $val;
                    $line .= join(' AND ', $when) . " THEN " . $this->db->quoteValue($conditionValue);
                }
                $line .= " END )";
                $lines[] = $line;
            } else {
                $setValue = !is_array($value) && isset($columnSchemas[$name])
                    ? $columnSchemas[$name]->dbTypecast($value)
                    : $value;

                if ($columnSchemas[$name]->phpType == 'string') {
                    $setValue = $this->db->quoteValue($setValue);
                }
                $lines[] = $this->db->quoteColumnName($name) . '=' . $setValue;
            }
        }

        $sql = 'UPDATE ' . $this->db->quoteTableName($table) . ' SET ' . implode(', ', $lines);

        $parts = [];
        foreach ($condition as $whereKey => $whereValue) {
            if (ArrayHelper::isTraversable($whereValue) || $whereValue instanceof Query) {
                $parts[] = $this->db->quoteColumnName($whereKey) . " IN ('" . join("', '", $whereValue) . "')";
            } else {
                $parts[] = $this->db->quoteColumnName($whereKey) . " = '" . $whereValue . "'";
            }
        }

        $where = join(' AND ', $parts);
        $this->setSql($where === '' ? $sql : $sql . ' WHERE ' . $where);
        return $this;
    }
}
