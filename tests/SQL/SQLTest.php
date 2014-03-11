<?php

namespace SQLTest;

use SQL\SQL;

class SQLTest extends \PHPUnit_Framework_TestCase {

	protected $db = null;

	private function pdo() {
		$this->db = new SQL('pgsql:host=localhost;port=5432;dbname=testing', 'postgres', '', true);
	}

	public function setUp() {
		$this->pdo();
		$this->db->exec('CREATE TABLE "t" ("id" serial primary key, "str_field" text, "bool_field" boolean, "int_field" integer)');
	}

	public function tearDown() {
		$this->db->exec('DROP TABLE "t" CASCADE');
		$this->db = null;
	}

	public function testConnectionException() {
		$this->setExpectedException('SQL\\Exception\\Connection');
		$db = new SQL('pgsql:host=localhost;port=5433;dbname=testing', 'postgres', '');
	}

	public function testExecQueryException() {
		$this->setExpectedException('SQL\\Exception\\Query');
		$this->db->exec('SELECT * FROM "r"');
	}

	public function testMultiExecQueryException() {
		$data = array(
			array('str_field' => 'value1'),
			array('str_field' => 'value2'),
			array('str_field' => 'value3')
		);
		$this->setExpectedException('SQL\\Exception\\Query');
		$this->db->multiExec('INSERT INTO "r" ("str_field") VALUES (:str_field)', $data);
	}

	public function testRawQueryException() {
		$this->setExpectedException('SQL\\Exception\\Query');
		$this->db->raw('SELECT * FROM "r"');
	}

	public function testLastStatementErrorCacheDisabledException() {
		$this->setExpectedException('SQL\\Exception\\CacheDisabled');
		$this->db->lastStatementError('tag');
	}

	public function testTagCacheDisabledException() {
		$this->setExpectedException('SQL\\Exception\\CacheDisabled');
		$this->db->tag();
	}

	public function testCountCacheDisabledException() {
		$this->setExpectedException('SQL\\Exception\\CacheDisabled');
		$this->db->count('tag');
	}

	public function testResultsCacheDisabledException() {
		$this->setExpectedException('SQL\\Exception\\CacheDisabled');
		$this->db->results('tag');
	}

	public function testNextCacheDisabledException() {
		$this->setExpectedException('SQL\\Exception\\CacheDisabled');
		$this->db->next('tag');
	}

	public function testLastConnectionError() {
		try {
			$this->db->exec('bad sql');
		} catch (\Exception $exception) {
			$error = $this->db->lastConnectionError();
			$this->assertStringEndsWith($error[2], $exception->getMessage());
			$error = $this->db->lastStatementError();
			$this->assertStringEndsWith($error[2], $exception->getMessage());
		}
	}

	public function testLastStatementError() {
		$this->pdo();
		$this->assertNull($this->db->lastStatementError());
		$this->db->cacheEnable();
		$this->assertNull($this->db->lastStatementError('tag'));
	}

	public function testTransactionBegin() {
		$this->assertFalse($this->db->transactionActive());
		$this->assertTrue($this->db->transactionBegin());
		$this->assertTrue($this->db->transactionActive());
		$this->assertFalse($this->db->transactionBegin());
		$this->db->transactionRollBack();
	}

	public function testTransactionActiveAndRollBack() {
		$data = array(
			'str_field' => 'value'
		);
		$this->assertTrue($this->db->transactionBegin());
		$this->db->exec('INSERT INTO "t" ("str_field") VALUES (:str_field)', $data);
		$this->assertTrue($this->db->transactionRollBack());
		$this->assertFalse($this->db->transactionActive());
		$this->assertFalse($this->db->transactionRollBack());
		$this->db->exec('SELECT * FROM "t"');
		$this->assertSame(0, $this->db->count());
	}

	public function testTransactionCommit() {
		$data = array(
			'str_field' => 'value'
		);
		$this->assertTrue($this->db->transactionBegin());
		$this->db->exec('INSERT INTO "t" ("str_field") VALUES (:str_field)', $data);
		$this->assertTrue($this->db->transactionCommit());
		$this->assertFalse($this->db->transactionActive());
		$this->assertFalse($this->db->transactionCommit());
		$this->db->exec('SELECT * FROM "t"');
		$this->assertSame(1, $this->db->count());
	}

	public function testParamBind() {
		$data = array(
			'str_field' => null,
			'int_field' => 10,
			'bool_field' => true
		);
		$this->db->exec('INSERT INTO "t" ("str_field", "int_field", "bool_field") VALUES (:str_field, :int_field, :bool_field)', $data);
		$this->db->exec('SELECT "str_field", "int_field", "bool_field" FROM "t"');
		$this->assertSame($data, $this->db->next());
	}

	public function testExecAndNext() {
		$data = array(
			'str_field' => 'value'
		);
		$this->assertTrue($this->db->exec('INSERT INTO "t" ("str_field") VALUES (:str_field)', $data));
		$this->assertTrue($this->db->exec('SELECT "str_field" FROM "t"'));
		$this->assertSame($data, $this->db->next());
	}

	public function testMultiExec() {
		$data = array(
			array('str_field' => 'value1'),
			array('str_field' => 'value2'),
			array('str_field' => 'value3')
		);
		$this->assertTrue($this->db->multiExec('INSERT INTO "t" ("str_field") VALUES (:str_field)', $data));
		$this->db->exec('SELECT "str_field" FROM "t"');
		$this->assertSame(3, $this->db->count());
		$this->assertSame($data, $this->db->results());
	}

	public function testLastId() {
		$data = array(
			'str_field' => 'value'
		);
		$this->db->exec('INSERT INTO "t" ("str_field") VALUES (:str_field)', $data);
		$this->assertSame('1', $this->db->lastId('t_id_seq'));
	}

	public function testCachedQuery() {
		$this->db->cacheEnable();
		$data = array(
			array('str_field' => 'value1'),
			array('str_field' => 'value2'),
			array('str_field' => 'value3')
		);
		$this->db->multiExec('INSERT INTO "t" ("str_field") VALUES (:str_field)', $data);
		$this->db->exec('SELECT "str_field" FROM "t" ORDER BY "id" ASC');
		$tag = $this->db->tag();
		$this->assertNotNull($tag);
		$this->assertGreaterThan(0, $this->db->count($tag));
		$this->assertSame(-1, $this->db->count('tag'));
		$this->assertSame($data[0], $this->db->next($tag));
		$this->assertSame(array(), $this->db->next('tag'));
		$this->assertSame(array_slice($data, 1), $this->db->results($tag));
		$this->assertSame(array(), $this->db->results('tag'));
		$this->db->cacheDisable();
	}
}