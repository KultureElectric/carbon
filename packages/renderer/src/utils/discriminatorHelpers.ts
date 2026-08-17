import { BytesEncoding, ConstantValueNode, isNode } from '@codama/nodes';
import { getBase16Encoder, getBase58Encoder, getBase64Encoder, getUtf8Encoder } from '@solana/codecs-strings';

export function getDiscriminatorBytes(constant: ConstantValueNode): number[] {
    if (isNode(constant.value, 'bytesValueNode')) {
        return encodedStringToBytes(constant.value.data, constant.value.encoding);
    } else if (isNode(constant.value, 'numberValueNode')) {
        const numberType = constant.type;
        if (isNode(numberType, 'numberTypeNode')) {
            return numberToBytes(constant.value.number, numberType.format, numberType.endian);
        }
    } else if (isNode(constant.value, 'stringValueNode') && isNode(constant.type, 'stringTypeNode')) {
        return encodedStringToBytes(constant.value.string, constant.type.encoding);
    }

    throw new Error(`Unsupported discriminator type: ${constant.value.kind}`);
}

function encodedStringToBytes(value: string, encoding: BytesEncoding): number[] {
    switch (encoding) {
        case 'base16':
            return Array.from(getBase16Encoder().encode(value.replace(/^0x/, '')));
        case 'base58':
            return Array.from(getBase58Encoder().encode(value));
        case 'base64':
            return Array.from(getBase64Encoder().encode(value));
        case 'utf8':
            return Array.from(getUtf8Encoder().encode(value));
    }
}

function numberToBytes(num: number | string | bigint, format: string, endian: 'be' | 'le'): number[] {
    let value = BigInt(num);
    const takeByte = (): number => {
        const byte = Number(value & 0xffn);
        value >>= 8n;
        return byte;
    };
    let bytes: number[];

    switch (format) {
        case 'u8':
            bytes = [takeByte()];
            break;
        case 'u16':
            bytes = [takeByte(), takeByte()];
            break;
        case 'u32':
            bytes = [takeByte(), takeByte(), takeByte(), takeByte()];
            break;
        case 'u64': {
            bytes = [];
            for (let i = 0; i < 8; i++) {
                bytes.push(takeByte());
            }
            break;
        }
        default:
            throw new Error(`Unsupported number format: ${format}`);
    }

    return endian === 'be' ? bytes.reverse() : bytes;
}
