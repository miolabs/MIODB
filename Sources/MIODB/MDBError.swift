//
//  File.swift
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
    case whereAlreadyDefined
    case whereNotFound
    case rootCanOnlyHaveOneGroupOrLine
    case usingDeprecatedFunctionWithNewFunctions
    case notSupported( _ message: String )
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
            case .whereAlreadyDefined:
                    return "[MDBError] Where clause already defined."
            case .whereNotFound:
                return "[MDBError] Where clause not found."
            case .rootCanOnlyHaveOneGroupOrLine:
                return "[MDBError] Root can only have one group or line."
            case .usingDeprecatedFunctionWithNewFunctions:
                return "[MDBError] Using deprecated function with new functions."
            case let .general( message ):
                return "[MDBError] \(message)."
            case let .notSupported( message ):
                return "[MDBError] \(message)."
        }
    }
}
