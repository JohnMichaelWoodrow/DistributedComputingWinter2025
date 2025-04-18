#region Copyright
/////////////////////////////////////////////////////////////////////////////////////////////
// Copyright 2024 Garmin International, Inc.
// Licensed under the Flexible and Interoperable Data Transfer (FIT) Protocol License; you
// may not use this file except in compliance with the Flexible and Interoperable Data
// Transfer (FIT) Protocol License.
/////////////////////////////////////////////////////////////////////////////////////////////
// ****WARNING****  This file is auto-generated!  Do NOT edit this file.
// Profile Version = 21.158.0Release
// Tag = production/release/21.158.0-0-gc9428aa
/////////////////////////////////////////////////////////////////////////////////////////////

#endregion

namespace Dynastream.Fit
{
    /// <summary>
    /// Implements the profile CameraEventType type as an enum
    /// </summary>
    public enum CameraEventType : byte
    {
        VideoStart = 0,
        VideoSplit = 1,
        VideoEnd = 2,
        PhotoTaken = 3,
        VideoSecondStreamStart = 4,
        VideoSecondStreamSplit = 5,
        VideoSecondStreamEnd = 6,
        VideoSplitStart = 7,
        VideoSecondStreamSplitStart = 8,
        VideoPause = 11,
        VideoSecondStreamPause = 12,
        VideoResume = 13,
        VideoSecondStreamResume = 14,
        Invalid = 0xFF


    }
}

