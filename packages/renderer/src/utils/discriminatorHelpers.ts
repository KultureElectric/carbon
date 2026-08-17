import { ConstantValueNode, isNode } from '@codama/nodes';

export function getDiscriminatorBytes(constant: ConstantValueNode): number[] {
    if (isNode(constant.value, 'bytesValueNode')) {
        return hexToBytes(constant.value.data);
    } else if (isNode(constant.value, 'numberValueNode')) {
        const numberType = constant.type;
        if (isNode(numberType, 'numberTypeNode')) {
            return numberToBytes(constant.value.number, numberType.format);
        }
    } else if (isNode(constant.value, 'stringValueNode') && isNode(constant.type, 'stringTypeNode')) {
        return stringToBytes(constant.value.string);
    }

    throw new Error(`Unsupported discriminator type: ${constant.value.kind}`);
}

function hexToBytes(hex: string): number[] {
    const cleanHex = hex.replace(/^0x/, '');
    const bytes: number[] = [];
    for (let i = 0; i < cleanHex.length; i += 2) {
        bytes.push(parseInt(cleanHex.substr(i, 2), 16));
    }
    return bytes;
}

function numberToBytes(num: number | string | bigint, format: string): number[] {
    let value = BigInt(num);
    const takeByte = (): number => {
        const byte = Number(value & 0xffn);
        value >>= 8n;
        return byte;
    };

    switch (format) {
        case 'u8':
            return [takeByte()];
        case 'u16':
            return [takeByte(), takeByte()];
        case 'u32':
            return [takeByte(), takeByte(), takeByte(), takeByte()];
        case 'u64': {
            const bytes: number[] = [];
            for (let i = 0; i < 8; i++) {
                bytes.push(takeByte());
            }
            return bytes;
        }
        default:
            throw new Error(`Unsupported number format: ${format}`);
    }
}

function stringToBytes(str: string): number[] {
    return Array.from(new TextEncoder().encode(str));
}
