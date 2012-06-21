<?php

/*
 * This file is part of the SQLite3 Driver for DBAL.
 *
 * (c) Jan Sorgalla <jsorgalla@googlemail.com>
 *
 * For the full copyright and license information, please view
 * the LICENSE file that was distributed with this source code.
 */

namespace Jsor\DBAL\Driver\Sqlite3;

use Doctrine\DBAL\Driver\Statement as StatementInterface;
use PDO;

/**
 * @author Jan Sorgalla <jsorgalla@googlemail.com>
 */
class Statement implements \IteratorAggregate, StatementInterface
{
    /**
     * @var array
     */
    protected static $_paramTypeMap = array(
        PDO::PARAM_STR  => \SQLITE3_TEXT,
        PDO::PARAM_BOOL => \SQLITE3_INTEGER,
        PDO::PARAM_NULL => \SQLITE3_NULL,
        PDO::PARAM_INT  => \SQLITE3_INTEGER,
        PDO::PARAM_LOB  => \SQLITE3_BLOB
    );

    /**
     * @var \SQLite3
     */
    protected $_conn;

    /**
     * @var \SQLite3Stmt
     */
    protected $_stmt;

    /**
     * @var \SQLite3Result
     */
    protected $_result;

    /**
     * @var integer
     */
    protected $_defaultFetchStyle = PDO::FETCH_BOTH;

    /**
     * Creates a new Statement that uses the given connection handle and SQL statement.
     *
     * @param \SQLite3 $conn
     * @param string   $prepareString
     */
    public function __construct(\SQLite3 $conn, $prepareString)
    {
        $this->_conn = $conn;
        $this->_stmt = $conn->prepare($prepareString);

        if (false === $this->_stmt) {
            throw new Exception($this->errorInfo(), $this->errorCode());
        }
    }

    /**
     * {@inheritdoc}
     */
    public function bindParam($column, &$variable, $type = null)
    {
        if ($type) {
            if (isset(self::$_paramTypeMap[$type])) {
                $type = self::$_paramTypeMap[$type];
            } else {
                throw new Exception("Unkown type: '{$type}'");
            }

            return $this->_stmt->bindParam($column, $variable, $type);
        }

        return $this->_stmt->bindParam($column, $variable);
    }

    /**
     * {@inheritdoc}
     */
    public function bindValue($param, $value, $type = null)
    {
        if ($type) {
            if (isset(self::$_paramTypeMap[$type])) {
                $type = self::$_paramTypeMap[$type];
            } else {
                throw new Exception("Unkown type: '{$type}'");
            }

            return $this->_stmt->bindValue($param, $value, $type);
        }

        return $this->_stmt->bindValue($param, $value);
    }

    /**
     * {@inheritdoc}
     */
    public function execute($params = null)
    {
        if (null !== $params) {
            foreach ($params as $param => $value) {
                $this->bindValue($param, $value);
            }
        }

        $this->_result = $this->_stmt->execute();

        if (!$this->_result) {
            throw new Exception($this->errorInfo(), $this->errorCode());
        }

        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function fetch($fetchStyle = null)
    {
        if (!$this->_result) {
            return null;
        }

        $fetchStyle = $fetchStyle ?: $this->_defaultFetchStyle;

        switch ($fetchStyle) {
            case PDO::FETCH_NUM:
                $row = $this->_result->fetchArray(\SQLITE3_NUM);
                break;

            case PDO::FETCH_ASSOC:
                $row = $this->_result->fetchArray(\SQLITE3_ASSOC);
                break;

            case PDO::FETCH_BOTH:
                $row = $this->_result->fetchArray(\SQLITE3_BOTH);
                break;

            default:
                throw new Exception("Unknown fetch type '{$fetchStyle}'");
        }

        return $row ?: null;
    }

    /**
     * {@inheritdoc}
     */
    public function fetchAll($fetchStyle = null)
    {
        $fetchStyle = $fetchStyle ?: $this->_defaultFetchStyle;

        $a = array();
        while (($row = $this->fetch($fetchStyle)) !== null) {
            $a[] = $row;
        }

        return $a;
    }

    /**
     * {@inheritdoc}
     */
    public function fetchColumn($columnIndex = 0)
    {
        $row = $this->fetch(PDO::FETCH_NUM);

        if (null === $row) {
            return null;
        }

        return $row[$columnIndex];
    }

    /**
     * {@inheritdoc}
     */
    public function errorCode()
    {
        return $this->_conn->lastErrorCode();
    }

    /**
     * {@inheritdoc}
     */
    public function errorInfo()
    {
        return $this->_conn->lastErrorMsg();
    }

    /**
     * {@inheritdoc}
     */
    public function closeCursor()
    {
        if (!$this->_result) {
            return false;
        }

        return $this->_result->finalize();
    }

    /**
     * {@inheritdoc}
     */
    public function rowCount()
    {
        return $this->_conn->changes();
    }

    /**
     * {@inheritdoc}
     */
    public function columnCount()
    {
        if (!$this->_result) {
            return 0;
        }

        return $this->_result->numColumns();
    }

    /**
     * {@inheritdoc}
     */
    public function setFetchMode($fetchMode = PDO::FETCH_BOTH)
    {
        $this->_defaultFetchStyle = $fetchMode;
    }

    /**
     * {@inheritdoc}
     */
    public function getIterator()
    {
        $data = $this->fetchAll($this->_defaultFetchStyle);

        return new \ArrayIterator($data);
    }
}
