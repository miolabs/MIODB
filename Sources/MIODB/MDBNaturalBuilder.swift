import Foundation

// MARK: - Query

public protocol QueryPart {
	//func add(to query: inout MDBQuery)
	func add(to query: MDBQuery) throws
}

public class Select : QueryPart {
	public var _selectFields: [ Any ] = []
	init (_ args: Any...) {
		let mdbQuery = MDBQuery("" )
		mdbQuery.select(args)
		self._selectFields = mdbQuery._selectFields
	}

	public func add(to query:  MDBQuery) throws {
		query._selectFields = self._selectFields
		query.queryType = .SELECT
	}
}

extension OrderBy : QueryPart {
	init ( _ field: String, _ dir: ORDER_BY_DIRECTION = .ASC) {
		self.field = field
		self.dir = dir
	}

	public func add(to query:  MDBQuery) throws {
		query.orderBy(self.field, self.dir)
	}
}

public class TableAlias : QueryPart {
	var alias: String
	init (_ alias: String) {
		self.alias = alias
	}

	public func add(to query:  MDBQuery) throws {
		query.tableAlias(self.alias)
	}
}
		
// MARK: - Where

public protocol WherePart {
	func add(to query: MDBQuery) throws
}

public class Condition : WherePart {
	let field: Any
	let op: WhereLineOperator
	let value: Any?
	init(_ field: Any, _ op: WhereLineOperator, _ value: Any? ) {
		self.field = field
		self.op = op
		self.value = value
	}

	public func add(to query:  MDBQuery) throws{
		try query.addCondition(field, op, value)
	}
}

public class Or : WherePart {
	var children: [WherePart] = []
	init(@NaturalWhere _ content: ( ) -> [WherePart]) {
		for part in content() {
			children.append(part)
		}
	}

	public func add(to query:  MDBQuery) throws{
		try query.beginOrGroup()
		for part in children {
			try part.add(to: query)
		}
		try query.endGroup()
	}
}

public class And : WherePart {
	var children: [WherePart] = []
	init(@NaturalWhere _ content: ( ) -> [WherePart]) {
		for part in content() {
			children.append(part)
		}
	}

	public func add(to query:  MDBQuery) throws{
		try query.beginAndGroup()
		for part in children {
			try part.add(to: query)
		}
		try query.endGroup()
	}
}

public class Where : QueryPart {
	public var _where : WherePart
	
	// We can't use the resultBuilder for exactly one element (as opposed to an array), we get compiling errors. 
	// init (@NaturalWhere _ content: ( ) -> WherePart) // <<<----- error
	init(content: @escaping () -> WherePart) {
		_where = content()
    }

	public func add(to query:  MDBQuery) throws{
		try query.where()
		try _where.add(to: query)		
	}
}

// MARK: - Builders

@resultBuilder
public struct NaturalWhere {

	public static func buildBlock(_ commands: WherePart...) -> [WherePart] {
		commands
	}
}

@resultBuilder
public struct NaturalQuery {

	public static func buildBlock(_ commands: QueryPart...) -> [QueryPart] {
		commands
	}
}

public extension MDBQuery {
	convenience init (_ table:String, @NaturalQuery _ content: ( ) -> [QueryPart]) throws {
		self.init(table)
		for part in content() {
			try part.add(to: self)
		}
    }
}
