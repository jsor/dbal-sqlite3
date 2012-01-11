<?php

/*
 * This file is part of the SQLite3 Driver for DBAL.
 *
 * (c) Jan Sorgalla <jsorgalla@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Jsor\DBAL\Driver\Sqlite3;

use Doctrine\DBAL\Driver\Connection as ConnectionInterface;
use SQLite3;

/**
 * @author Jan Sorgalla <jsorgalla@googlemail.com>
 */
class Connection implements ConnectionInterface
{
    /**
     * @var \SQLite3
     */
    private $_conn;

    public function __construct($filename, $flags = null, $encryptionKey = null, $busyTimeout = null, array $userDefinedFunctions = array(), array $userDefinedExtensions = array())
    {
        if (null === $flags) {
            $flags = \SQLITE3_OPEN_READWRITE | \SQLITE3_OPEN_CREATE;
        }

        $this->_conn = new SQLite3($filename, $flags, $encryptionKey);

        if (null !== $busyTimeout) {
            $this->_conn->busyTimeout($busyTimeout);
        } else {
            $this->_conn->busyTimeout(60000);
        }

        foreach ($userDefinedFunctions as $fn => $data) {
            $this->_conn->createFunction($fn, $data['callback'], $data['numArgs']);
        }

        foreach ($userDefinedExtensions as $extension) {
            $this->_conn->loadExtension($extension);
        }
    }

    /**
     * Retrieve mysqli native resource handle.
     *
     * Could be used if part of your application is not using DBAL
     *
     * @return mysqli
     */
    public function getWrappedResourceHandle()
    {
        return $this->_conn;
    }

    /**
     * {@inheritdoc}
     */
    public function prepare($prepareString)
    {
        return new Statement($this->_conn, $prepareString);
    }

    /**
     * {@inheritdoc}
     */
    public function query()
    {
        $args = func_get_args();
        $sql = $args[0];
        $stmt = $this->prepare($sql);
        $stmt->execute();
        return $stmt;
    }

    /**
     * {@inheritdoc}
     */
    public function quote($input, $type = \PDO::PARAM_STR)
    {
        if (is_int($input) || is_float($input)) {
            return $input;
        }

        return "'" . $this->_conn->escapeString($input) . "'";
    }

    /**
     * {@inheritdoc}
     */
    public function exec($statement)
    {
        $this->_conn->exec($statement);
        return $this->_conn->changes();
    }

    /**
     * {@inheritdoc}
     */
    public function lastInsertId($name = null)
    {
        return $this->_conn->lastInsertRowID();
    }

    /**
     * {@inheritdoc}
     */
    public function beginTransaction()
    {
        $this->_conn->query('BEGIN TRANSACTION');
        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function commit()
    {
        return $this->_conn->query('COMMIT');
    }

    /**
     * {@inheritdoc}non-PHPdoc)
     */
    public function rollBack()
    {
        return $this->_conn->query('ROLLBACK');
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
}
