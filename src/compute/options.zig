const std = @import("std");
const core = @import("core.zig");

const DataType = core.DataType;

/// Well-known options payload tags used by compute kernels.
pub const OptionsTag = enum {
    /// Kernel does not require options.
    none,
    /// Type conversion / cast behavior.
    cast,
    /// Arithmetic behavior (overflow, divide-by-zero, etc.).
    arithmetic,
    /// Filter behavior.
    filter,
    /// Sort behavior (order, null placement, NaN handling, stability).
    sort,
    /// Escape hatch for downstream custom kernels.
    custom,
};

/// Common options for cast-like kernels.
pub const CastOptions = struct {
    /// If true, conversion must fail on lossy/overflowing cast.
    safe: bool = true,
    /// Optional target type override used by cast kernels.
    to_type: ?DataType = null,
};

/// Common options for arithmetic kernels.
pub const ArithmeticOptions = struct {
    /// If true, overflow should produce an error instead of wrapping.
    check_overflow: bool = true,
    /// If true, division by zero should return an error.
    divide_by_zero_is_error: bool = true,
};

/// Common options for filter-like kernels.
pub const FilterOptions = struct {
    /// If true, nulls in predicate are treated as false and dropped.
    drop_nulls: bool = true,
};

/// Sort direction for sort-like kernels.
pub const SortOrder = enum {
    ascending,
    descending,
};

/// Null placement strategy for sort-like kernels.
pub const SortNullPlacement = enum {
    at_start,
    at_end,
};

/// NaN placement strategy for floating-point sort-like kernels.
pub const SortNaNPlacement = enum {
    at_start,
    at_end,
};

/// Common options for sort-like kernels.
pub const SortOptions = struct {
    /// Ordering direction.
    order: SortOrder = .ascending,
    /// Null placement behavior.
    null_placement: SortNullPlacement = .at_end,
    /// Optional NaN placement policy for floating-point inputs.
    nan_placement: ?SortNaNPlacement = null,
    /// If true, equal-key ordering should be stable.
    stable: bool = false,
};

/// Custom untyped options hook for downstream extension kernels.
pub const CustomOptions = struct {
    /// Caller-defined discriminator.
    tag: []const u8,
    /// Optional opaque payload owned by caller.
    payload: ?*const anyopaque = null,
};

/// Type-safe options payload passed to all kernel signatures and executors.
pub const Options = union(OptionsTag) {
    none: void,
    cast: CastOptions,
    arithmetic: ArithmeticOptions,
    filter: FilterOptions,
    sort: SortOptions,
    custom: CustomOptions,

    pub fn noneValue() Options {
        return .{ .none = {} };
    }

    pub fn tag(self: Options) OptionsTag {
        return std.meta.activeTag(self);
    }
};
