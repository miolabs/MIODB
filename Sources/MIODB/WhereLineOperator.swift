
import Foundation

public enum WhereLineOperator {
    // C-like
    //@available(*, deprecated, message: "Use .equal") This fills our code with warnings that we cant get rid of because we need to use the old names for backward compatibility
    case EQ
    case NEQ
    case LT
    case LE
    case GT
    case GE
    case NOT_IN
    case IN
    case IS
    case IS_NOT
    case LIKE
    case ILIKE
    case JSON_EXISTS_IN
    case RAW
    case BITWISE_AND
    case BITWISE_OR
    case BITWISE_XOR
    case BITWISE_NOT
    // swifty
    case equal
    case notEqual
    case lessThan
    case lessThanOrEqual
    case greaterThan
    case greaterThanOrEqual
    case notIn
    case `in`
    case `is`
    case isNot
    case like
    case ilike
    case jsonExistsIn
    case raw
    case bitwiseAnd
    case bitwiseOr
    case bitwiseXor
    case bitwiseNot

    var value: String {
        switch self {
            case .EQ: return "="
            case .NEQ: return "!="
            case .LT: return "<"
            case .LE: return "<="
            case .GT: return ">"
            case .GE: return ">="
            case .NOT_IN: return "NOT IN"
            case .IN: return "IN"
            case .IS: return "IS"
            case .IS_NOT: return "IS NOT"
            case .LIKE: return "LIKE"
            case .ILIKE: return "ILIKE"
            case .JSON_EXISTS_IN: return "?|"
            case .RAW: return ""
            case .BITWISE_AND: return "&"
            case .BITWISE_OR: return "|"
            case .BITWISE_XOR: return "#"
            case .BITWISE_NOT: return "~"

            case .equal: return "="
            case .notEqual: return "!="
            case .lessThan: return "<"
            case .lessThanOrEqual: return "<="
            case .greaterThan: return ">"
            case .greaterThanOrEqual: return ">="
            case .notIn: return "NOT IN"
            case .in: return "IN"
            case .is: return "IS"
            case .isNot: return "IS NOT"
            case .like: return "LIKE"
            case .ilike: return "ILIKE"
            case .jsonExistsIn: return "?|"
            case .raw: return ""
            case .bitwiseAnd: return "&"
            case .bitwiseOr: return "|"
            case .bitwiseXor: return "#"
            case .bitwiseNot: return "~"
        }
    }

}

public typealias WHERE_LINE_OPERATOR = WhereLineOperator  // for backward compatibility


// public enum WhereLineOperator: String {
//     case EQ = "="
//     case NEQ = "!="
//     case LT = "<"
//     case LE = "<="
//     case GT = ">"
//     case GE = ">="
//     case NOT_IN = "NOT IN"
//     case IN = "IN"
//     case IS = "IS"
//     case IS_NOT = "IS NOT"
//     case LIKE = "LIKE"
//     case ILIKE = "ILIKE"
//     case JSON_EXISTS_IN = "?|"
//     case RAW = ""
//     case BITWISE_AND = "&"
//     case BITWISE_OR = "|"
//     case BITWISE_XOR = "#"
//     case BITWISE_NOT = "~"
// }
