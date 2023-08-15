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
        }
    }
}
