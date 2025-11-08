import torch
import librosa
import soundfile as sf

def load_and_process(path: str, target_sr: int = 16000):
    """
    Load an audio file, convert to mono, resample, and return as a torch tensor.

    Args:
        path (str): Path to audio file.
        target_sr (int): Target sample rate (default: 16000).

    Returns:
        waveform (torch.Tensor): Audio waveform as a 1D torch tensor.
        target_sr (int): The sample rate of the returned waveform.
    """
    data, samplerate = sf.read(path, dtype="float32")
    waveform = torch.from_numpy(data)

    # Stereo → mono
    if waveform.ndim > 1:
        waveform = waveform.mean(dim=1)

    # Resample
    waveform_np = waveform.numpy()
    waveform_resampled = librosa.resample(
        waveform_np, orig_sr=samplerate, target_sr=target_sr
    )
    waveform = torch.from_numpy(waveform_resampled)

    return waveform, target_sr


def chunk_mono_audio(wav, sr, chunk_duration=120, overlap=30):
    """Split audio into overlapping chunks(seconds)"""
    chunk_samples = int(chunk_duration * sr)
    overlap_samples = int(overlap * sr)
    total_samples = len(wav)

    chunks = []
    start = 0
    while start < total_samples:
        end = min(start + chunk_samples, total_samples)
        chunk = wav[start:end]
        chunks.append((chunk, sr, start/sr, end/sr))
        if end == total_samples:
            break
        start += chunk_samples - overlap_samples
    return chunks
