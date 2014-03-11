<?php
/**
 *
 * Abstraction to SQL queries by using a PDO Wrapper
 *
 * @copyright 2014 appdeck
 * @link http://github.com/appdeck/sql
 * @license http://www.gnu.org/licenses/gpl-3.0.html GNU General Public License, version 3
 */

namespace SQL;

use SQL\Exception;

class SQL {
	/**
	 * @var PDO Instance
	 */
	private $pdo;
	/**
	 * @var Last executed PDO Statement
	 */
	private $statement = null;
	/**
	 * @var Last executed Query's Tag ID
	 */
	private $tag = null;
	/**
	 * @var Query Cache
	 */
	private $cache = null;
	/**
	 * @var Last executed SQL Query
	 */
	private $lastQuery = null;

	/**
	 * Class constructor
	 *
	 * @param string $dsn
	 * @param string $user
	 * @param string $pass
	 * @param boolean $pool
	 * @return void
	 * @throws Exception\Connection
	 */
	public function __construct($dsn, $user, $pass, $pool = false) {
		try {
			$options = array(
				\PDO::ATTR_EMULATE_PREPARES => false,
				\PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION
			);
			if ($pool)
				$options[\PDO::ATTR_PERSISTENT] = true;
			$this->pdo = new \PDO($dsn, $user, $pass, $options);
		} catch (\Exception $exception) {
			throw new Exception\Connection($exception->getMessage());
		}
	}

	/**
	 * Enables Query cache
	 *
	 * @return void
	 */
	public function cacheEnable() {
		if (is_null($this->cache))
			$this->cache = array();
	}

	/**
	 * Disables Query cache
	 *
	 * @return void
	 */
	public function cacheDisable() {
		$this->cache = null;
	}

	/**
	 * Starts a transaction block
	 *
	 * @return boolean
	 */
	public function transactionBegin() {
		if ($this->pdo->inTransaction())
			return false;
		return $this->pdo->beginTransaction();
	}

	/**
	 * Returns the current transaction block state
	 *
	 * @return boolean
	 */
	public function transactionActive() {
		return $this->pdo->inTransaction();
	}

	/**
	 * Commits a transaction block
	 *
	 * @return boolean
	 */
	public function transactionCommit() {
		if ($this->pdo->inTransaction())
			return $this->pdo->commit();
		return false;
	}

	/**
	 * Cancels a transaction block
	 *
	 * @return boolean
	 */
	public function transactionRollBack() {
		if ($this->pdo->inTransaction())
			return $this->pdo->rollBack();
		return false;
	}

	/**
	 * Binds parameters and values to the statement handler
	 *
	 * @param array $markers an array with placeholder => value
	 * @return void
	 */
	private function bind(array $markers) {
		foreach ($markers as $marker => $value) {
			if (is_null($value))
				$type = \PDO::PARAM_NULL;
			else if (is_numeric($value))
				$type = \PDO::PARAM_INT;
			else if (is_bool($value))
				$type = \PDO::PARAM_BOOL;
			else
				$type = \PDO::PARAM_STR;
			$this->statement->bindValue($marker, $value, $type);
		}
	}

	/**
	 * Executes a SQL query with a single data set
	 *
	 * @param string $sql SQL query to be executed
	 * @param array $markers an array with placeholder => value
	 * @return boolean
	 * @throws Exception\Query
	 */
	public function exec($sql, array $markers = array()) {
		try {
			$this->lastQuery = $sql;
			if (is_null($this->cache)) {
				$this->statement = $this->pdo->prepare($sql);
				if ($this->statement === false)
					return false;
			} else {
				$this->tag = md5($sql);
				if (isset($this->cache[$this->tag]))
					$this->statement = $this->cache[$this->tag];
				else {
					$this->statement = $this->pdo->prepare($sql);
					if ($this->statement === false)
						return false;
					$this->cache[$this->tag] = $this->statement;
				}
			}
			if (count($markers))
				$this->bind($markers);
			return $this->statement->execute();
		} catch (\Exception $exception) {
			throw new Exception\Query($exception->getMessage());
		}
	}

	/**
	 * Executes a SQL query with multiple data sets
	 *
	 * @param string $sql SQL query to be executed
	 * @param array $markers an array with placeholder => value
	 * @return boolean
	 * @throws Exception\Query
	 */
	public function multiExec($sql, array $markers) {
		try {
			$this->lastQuery = $sql;
			if (is_null($this->cache)) {
				$this->statement = $this->pdo->prepare($sql);
				if ($this->statement === false)
						return false;
			} else {
				$this->tag = md5($sql);
				if (isset($this->cache[$this->tag]))
					$this->statement = $this->cache[$this->tag];
				else {
					$this->statement = $this->pdo->prepare($sql);
					if ($this->statement === false)
						return false;
					$this->cache[$this->tag] = $this->statement;
				}
			}
			$flag = true;
			foreach ($markers as $internal) {
				$this->bind($internal);
				$flag |= $this->statement->execute();
			}
			return (bool)$flag;
		} catch (\Exception $exception) {
			throw new Exception\Query($exception->getMessage());
		}
	}

	/**
	 * Executes a raw SQL query
	 *
	 * @param string $sql SQL query to be executed
	 * @return mixed
	 * @throws Exception\Query
	 */
	public function raw($sql) {
		try {
			$this->lastQuery = $sql;
			return $this->pdo->exec($sql);
		} catch (\Exception $exception) {
			throw new Exception\Query($exception->getMessage());
		}
	}

	/**
	 * Returns an extended error information associated with the last operation on the database handle
	 *
	 * @return array
	 */
	public function lastConnectionError() {
		return $this->pdo->errorInfo();
	}

	/**
	 * Returns an extended error information associated with the last operation on the statement handle
	 *
	 * @param string $tag Statement tag name
	 * @return array|null
	 * @throws Exception\CacheDisabled
	 */
	public function lastStatementError($tag = null) {
		if (is_null($tag)) {
			if (is_null($this->statement))
				return null;
			return $this->statement->errorInfo();
		}
		if (is_null($this->cache))
			throw new Exception\CacheDisabled;
		if (isset($this->cache[$tag]))
			return $this->cache[$tag]->errorInfo;
		return null;
	}

	/**
	 * Returns last inserted id
	 *
	 * @param $sequence Sequence name
	 * @return string
	 */
	public function lastId($sequence = null) {
		return $this->pdo->lastInsertId($sequence);
	}

	/**
	 * Tags the current statement
	 *
	 * @return string
	 * @throws Exception\CacheDisabled
	 */
	public function tag() {
		if (is_null($this->cache))
			throw new Exception\CacheDisabled;
		if (!isset($this->cache[$this->tag]))
			$this->cache[$this->tag] = $this->statement;
		return $this->tag;
	}

	/**
	 * Returns the number of rows affected by INSERT/UPDATE/DELETE queries
	 *
	 * @return int
	 * @throws Exception\CacheDisabled
	 */
	public function count($tag = null) {
		if (is_null($tag)) {
			if (is_null($this->statement))
				return -1;
			return $this->statement->rowCount();
		}
		if (is_null($this->cache))
			throw new Exception\CacheDisabled;
		if (isset($this->cache[$tag]))
			return $this->cache[$tag]->rowCount();
		return -1;
	}

	/**
	 * Fetches all the results from a SELECT query
	 *
	 * @param string $tag Statement tag name
	 * @return array
	 * @throws Exception\CacheDisabled
	 */
	public function results($tag = null) {
		if (is_null($tag)) {
			if (is_null($this->statement))
				return array();
			return $this->statement->fetchAll(\PDO::FETCH_ASSOC);
		}
		if (is_null($this->cache))
			throw new Exception\CacheDisabled;
		if (isset($this->cache[$tag]))
			return $this->cache[$tag]->fetchAll(\PDO::FETCH_ASSOC);
		return array();
	}

	/**
	 * Fetches the next result from a SELECT query
	 *
	 * @param string $tag Statement tag name
	 * @return array
	 * @throws Exception\CacheDisabled
	 */
	public function next($tag = null) {
		if (is_null($tag)) {
			if (is_null($this->statement))
				return array();
			return $this->statement->fetch(\PDO::FETCH_ASSOC);
		}
		if (is_null($this->cache))
			throw new Exception\CacheDisabled;
		if (isset($this->cache[$tag]))
			return $this->cache[$tag]->fetch(\PDO::FETCH_ASSOC);
		return array();
	}
}
