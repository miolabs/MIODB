//
//  MDBCapabilities.swift
//  MIODB
//
//  Created by Javier Segura Perez on 25/07/2026.
//  Copyright © 2026 Javier Segura Perez. All rights reserved.
//

import Foundation

/// Capability of backends that connect over the network and therefore need
/// credentials (PostgreSQL, MySQL, Oracle). Embedded backends like SQLite
/// don't conform: their whole configuration is `database` (the file path).
/// Storage lives in `MDBNetworkConnection` / `MIONetworkDB`.
public protocol MDBCredentials : AnyObject
{
    var host: String?     { get set }
    var port: Int32?      { get set }
    var user: String?     { get set }
    var password: String? { get set }
}

/// Capability of backends that support in-database namespaces selected per
/// connection (PostgreSQL schemas via `search_path`). Operationally a scheme
/// is only used to select a venue at connect time; `changeScheme` remains for
/// the few admin paths that iterate over all venues.
///
/// This is the legacy compartment of the venue-per-schema model: once venues
/// migrate to one database per venue (selected with `create(to_db:)`), no
/// operational code needs this protocol and backends can drop the conformance.
public protocol MDBSchemes : AnyObject
{
    var scheme: String? { get set }
    func changeScheme( _ scheme: String? ) throws
}
