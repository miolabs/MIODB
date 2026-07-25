//
//  MDBNetworkConnection.swift
//  MIODB
//
//  Created by Javier Segura Perez on 25/07/2026.
//  Copyright © 2026 Javier Segura Perez. All rights reserved.
//

import Foundation

/// Base connection factory for network backends. Backend factories that need
/// credentials (`MDBPostgreConnection`, MySQL, Oracle) should subclass this
/// instead of `MDBConnection`.
///
/// Stage A: credentials and scheme storage are still inherited from
/// `MDBConnection`. Stage B moves the stored properties here and strips them
/// from the base class, so credential-less backends (SQLite) don't carry them.
open class MDBNetworkConnection : MDBConnection, MDBCredentials
{
}
