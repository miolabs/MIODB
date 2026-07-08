//
//  MDBError.swift
//  
//
//  Created by David Trallero on 26/11/2020.
//

import Foundation


public enum MDBError: Error
{
    case general( _ message: String )
    case cantBeginTransactionWhileInsideTransaction
    case cantEndTransactionWhileNotInsideTransaction
    case invalidPoolID( _ poolID: String, functionName: String = #function)
    case createNotImplemented( functionName: String = #function )
    case columnNotFound( _ column: String )
    case nullValue( _ column: String )
    case typeMismatch( _ column: String, _ expectedType: String )
    case conversionFailed( _ expectedType: String, _ rawValue: String )
}


extension MDBError: LocalizedError {
    public var errorDescription: String? {
        switch self {
            case .cantBeginTransactionWhileInsideTransaction:
                return "[MIODBError] Can't begin transaction, another transaction is already in progress."
            case .cantEndTransactionWhileNotInsideTransaction:
                return "[MIODBError] Can't end transaction if a transaction is not in progress."
            case let .invalidPoolID(poolID, functionName):
                return "[MDBError] \(poolID) does not exists in MDBManager. Called from \"\(functionName)\"."
            case let .createNotImplemented(functionName):
                return "[MDBError] create function not implemented in MDBConnection. Called from \"\(functionName)\"."
        case let .general( message ):
            return "[MDBError] \(message)."
            case let .columnNotFound( column ):
                return "[MDBError] Column \"\(column)\" not found in the query result."
            case let .nullValue( column ):
                return "[MDBError] Column \"\(column)\" is NULL."
            case let .typeMismatch( column, expectedType ):
                return "[MDBError] Column \"\(column)\" can't be converted to \(expectedType)."
            case let .conversionFailed( expectedType, rawValue ):
                return "[MDBError] Value \"\(rawValue)\" can't be converted to \(expectedType)."
        }
    }
}
