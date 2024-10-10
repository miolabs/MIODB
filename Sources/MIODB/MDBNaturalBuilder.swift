
//
// Module to build MDBQuery objects using a declarative syntax rather than method chaining (both are valid)
// See TestNaturalBuilder.swift for examples
//

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

public class Insert : QueryPart {
	var values: [String: Any?] = [:]
	var multiValues: [[String: Any?]] = []
	init (_ args: [String: Any?]) {
		self.values = args
	}
	init (_ args: [[String: Any?]]) {
		self.multiValues = args
	}

	public func add(to query:  MDBQuery) throws {
		if multiValues.isEmpty {
			try query.insert(values)
		} else {
			try query.insert(multiValues)
		}
	}
}

public class Update : QueryPart {
	var values: [String: Any?] = [:]
	init (_ args: [String: Any?]) {
		self.values = args
	}

	public func add(to query:  MDBQuery) throws {
		try query.update(values)
	}
}

public class Join : QueryPart {
	var table1: String
	var from1: String?
	var to1: String
	var joinType1: JOIN_TYPE
	var as_what1: String?
	init (table: String, from: String? = nil, to: String, joinType: JOIN_TYPE = .INNER, as as_what: String? = nil) {
		self.table1 = table
		self.from1 = from
		self.to1 = to
		self.joinType1 = joinType
		self.as_what1 = as_what
	}

	public func add(to query:  MDBQuery) throws {
		try query.join(table: table1, from: from1, to: to1, joinType: joinType1, as: as_what1)
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

public class GroupBy : QueryPart {
	var group: String
	init (_ group: String) {
		self.group = group
	}

	public func add(to query:  MDBQuery) throws {
		query.groupBy(group)
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

public class Limit : QueryPart {
	var value: Int32
	init (_ value: Int32) {
		self.value = value
	}

	public func add(to query:  MDBQuery) throws {
		query.limit(value)
	}
}

public class Offset : QueryPart {
	var value: Int32
	init (_ value: Int32) {
		self.value = value
	}

	public func add(to query:  MDBQuery) throws {
		query.offset(value)
	}
}

public class Returning : QueryPart {
	var fields: [String] = []
	init (_ args: String...) {
		for field in args {
            fields.append( MDBValue( fromTable: field ).value)
        }
	}

	public func add(to query:  MDBQuery) throws {
		query._returning = self.fields
	}
}

public class Test : QueryPart {
	init () {}
	public func add(to query:  MDBQuery) throws {
		query.test()
	}
}

public class DistinctOn : QueryPart {
	var cols: [String] = []
	init (_ cols: [String]) {
		self.cols = cols
	}

	public func add(to query:  MDBQuery) throws {
		query.distinctOn(self.cols)
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

public typealias ConditionTuple = (Any, WhereLineOperator, Any?)

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

	init(content: @escaping () -> ConditionTuple) {
		let value = content()
		_where = Condition(value.0, value.1, value.2)
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

/* Experiments for a parentheses free syntax in the conditions
	public static func buildBlock(_ commands: Any?...) -> [WherePart] {
		var ret: [WherePart] = []
		var currentField: Any? = nil
		var currentOp: WhereLineOperator? = nil
		var currentValue: Any? = nil
		for command in commands {
			if let command = command as? WherePart {
				ret.append(command)
			}
			else if let command = command as? String {
				if currentField == nil {
					currentField = command
				} else {
					currentValue = command
				}
			}
			else if let command = command as? WhereLineOperator {
				currentOp = command
			}
			else{
				currentValue = command == nil ? NSNull() : command 
			}
			
			if currentField != nil && currentOp != nil && currentValue != nil {
				ret.append(Condition(currentField!, currentOp!, currentValue is NSNull ? nil : currentValue))
				currentField = nil
				currentOp = nil
				currentValue = nil
			}
		}

		return ret
	}
	*/

	public static func buildExpression(_ value: ConditionTuple) -> Condition {
		Condition(value.0, value.1, value.2)	
	}

	public static func buildExpression(_ value: WherePart) -> WherePart {
		value	
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
