//
//  MIONetworkDB.swift
//  MIODB
//
//  Created by Javier Segura Perez on 25/07/2026.
//  Copyright © 2026 Javier Segura Perez. All rights reserved.
//

import Foundation

/// Base class for network database backends. `MIODBPostgreSQL`, MySQL and
/// Oracle should subclass this instead of `MIODB`, gaining the credential and
/// scheme capabilities. Embedded backends (SQLite) subclass `MIODB` directly
/// and conform to neither protocol.
///
/// Stage A: storage and `changeScheme` are still inherited from the base
/// classes. Stage B moves them here and strips them from `MDBConnection` /
/// `MIODB`.
open class MIONetworkDB : MIODB, MDBCredentials, MDBSchemes
{
}
