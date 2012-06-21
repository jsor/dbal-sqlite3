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

use Doctrine\DBAL\Driver as DriverInterface;

/**
 * @author Jan Sorgalla <jsorgalla@googlemail.com>
 */
class Driver implements DriverInterface
{
    /**
     * @var array
     */
    protected $_userDefinedFunctions = array(
        'sqrt'    => array('callback' => array('Doctrine\DBAL\Platforms\SqlitePlatform', 'udfSqrt'), 'numArgs' => 1),
        'mod'     => array('callback' => array('Doctrine\DBAL\Platforms\SqlitePlatform', 'udfMod'), 'numArgs' => 2),
        'locate'  => array('callback' => array('Doctrine\DBAL\Platforms\SqlitePlatform', 'udfLocate'), 'numArgs' => -1),
    );

    /**
     * @var array
     */
    protected $_userDefinedExtensions = array();

    /**
     * Tries to establish a database connection to SQLite.
     *
     * @param  array      $params
     * @param  string     $username
     * @param  string     $password
     * @param  array      $driverOptions
     * @return Connection
     */
    public function connect(array $params, $username = null, $password = null, array $driverOptions = array())
    {
        if (isset($driverOptions['userDefinedFunctions'])) {
            $this->_userDefinedFunctions = array_merge(
                $this->_userDefinedFunctions, $driverOptions['userDefinedFunctions']);
        }

        if (isset($driverOptions['userDefinedExtensions'])) {
            $this->_userDefinedExtensions = array_merge(
                $this->_userDefinedExtensions,
                $driverOptions['userDefinedExtensions']
            );
        }

        if (isset($params['dbname'])) {
            $filename = $params['dbname'];
        } elseif (isset($params['path'])) {
            $filename = $params['path'];
        } elseif (isset($params['memory'])) {
            $filename = ':memory:';
        } else {
            throw new Exception('Either a dbname, path or a memory entry is required in $params');
        }

        $connection = new Connection(
            $filename,
            isset($params['flags']) ? $params['flags'] : null,
            isset($params['encryptionKey']) ? $params['encryptionKey'] : null,
            isset($params['busyTimeout']) ? $params['busyTimeout'] : null,
            $this->_userDefinedFunctions,
            $this->_userDefinedExtensions
        );

        return $connection;
    }

    /**
     * {@inheritdoc}
     */
    public function getDatabasePlatform()
    {
        return new \Doctrine\DBAL\Platforms\SqlitePlatform();
    }

    /**
     * {@inheritdoc}
     */
    public function getSchemaManager(\Doctrine\DBAL\Connection $conn)
    {
        return new \Doctrine\DBAL\Schema\SqliteSchemaManager($conn);
    }

    /**
     * {@inheritdoc}
     */
    public function getName()
    {
        return 'sqlite3';
    }

    /**
     * {@inheritdoc}
     */
    public function getDatabase(\Doctrine\DBAL\Connection $conn)
    {
        $params = $conn->getParams();

        if (isset($params['dbname'])) {
            return $params['dbname'];
        }

        if (isset($params['path'])) {
            return $params['path'];
        }

        return null;
    }
}
